# module SQLTable

# export Table, Database, initDB, getTable, getAllRows, getColumn, getRowsWhere, 


# Module SQLTable is a component for a mock SQL query processor.
# A directory containing csv files acts as a database.
# SQLTable defines the Table and Database structs, as well as some
# convenience functions


struct Table
    name::String
    columns::Dict{Any, Int64}
    rows::Vector{Vector{Any}}

    "Empty table constructor"
    Table() = new()
    
    "Construct Initial Tables, used in initDB"
    function Table(name, rows)
        new(name, 
            Dict(Symbol(f) => i for (i, f) in enumerate(rows[1])), 
            map(r -> String.(r), rows[2:end]))
    end

    "Construct a table with complete rows and a vector of symbols"
    function Table(name, columns::Vector{Symbol}, rows)
        new(name,
            Dict(Symbol(f) => i for (i, f) in enumerate(columns)),
            rows)
    end

    "Construct a table from an existing table"
    function Table(name::String, columns::Dict{Symbol, Int64}, rows::Vector{Vector{Any}})
        new(name, columns, rows)
    end

    "Fixed something at some point, may be useless now"
    function Table(name::Any, columns::Any, rows::Any)
        new(name, columns, rows)
    end
end

struct Database # just a default constructor
    name::String
    tables::Dict{Symbol, Table}
    files::Vector{String}
end

"Initializes the database and all table structures"
function initDB(dbname)
    dbstring = "./databases/$dbname/"
    
    if Sys.iswindows()
        dbstring = ".\\databases\\$dbname\\"
    end
    
    if !(dbname in readdir("databases"))
        error("Database $dbname does not exist")
    end
    
    dbfiles = readdir(dbstring)
    tables = Dict{Symbol, Table}()
    for f in dbfiles
        if f == "temp"
            continue
        end
        tblname = f[1:end-4]
        rowdata = split.(readlines(open("$dbstring$f")), "|")
        tables[Symbol(tblname)] = Table(tblname, rowdata)
    end
    
    Database(dbname, tables, dbfiles)
end

"Reads in bar (|) separated csv files and converts to Table objects"
function readBarCSV(db, filename)
    tblname = filename[1:end-4]
    rowdata = split.(readlines(open("./databases/$(db.name)/temp/$filename")), "|")
    Table(tblname, rowdata)
end

"Writes a bar separated CSV file to the database directory"
function writeBarCSV(db, tbl)
    filename = tbl.name * ".csv"
    f = open("./databases/$(db.name)/temp/$filename", write=true)
    columns = map(x -> x[1], sort_columns(tbl.columns))
    for key in columns
        write(f, "$key")
        key ≠ columns[end] && write(f, "|")
    end
    write(f, "\n")
    for row in tbl.rows
        for cell in row
            write(f, "$cell")
            cell ≠ row[end] && write(f, "|")
        end
        write(f, "\n")
    end
    close(f)
end


"Pretty Printing for Table structs"
function displayTable(tbl)
    ncols = length(tbl.columns)
    width = length(tbl.name) + 12
    println("*"^width)
    println("*  $(tbl.name) table  *")
    println("*"^width)
    width = 30
    for key in map(x -> x[1], sort_columns(tbl.columns))
        spaces = width - length(String(key))
        print(" $key" * " "^spaces)
    end
    println()
    println("-"^(width * ncols))
    for row in tbl.rows
        for cell in row
            spaces = width - length(cell)
            print("|$cell" * " "^spaces)
        end
        println()
        println("-"^(width * ncols))
    end
end


# Convenience Functions
getTable(db::Database, tblname::Symbol) = db.tables[tblname]
getRows(tbl::Table) = tbl.rows
getColumns(tbl::Table) = tbl.columns
getColumn(tbl::Table, column::Symbol) = [r[tbl.columns[column]] for r in tbl.rows]
getRowsWhere(tbl, column, value) = filter(r -> r[tbl.columns[column]] == value, getAllRows(tbl))


# Functions for Sorting Column Keys
dictToVector(d) = [[k, v] for (k,v) in d]
columns_lt(x, y) = x[2] < y[2]
sort_columns(cols) = sort(dictToVector(cols), lt=columns_lt)


"Projection Operator"
function projection(tbl::Table, tempName::String, columns::Symbol...)
    #=
    if length(columns) == 1
        col = columns[1]
        return Table(tempName, columns, getColumn(tbl, col))
    end
    =#
    newheader = [columns[1]]
    newvalues = getColumn(tbl, columns[1])
    for column in columns[2:end]
        push!(newheader, column)
        newvalues = hcat(newvalues, getColumn(tbl, column))
    end
    newrows = [newvalues[i, :] for i in 1:size(newvalues)[1]]
    
    Table(tempName,
          newheader,
          newrows)
end

"Returns the complement table given a key to be excluded"
function projectComplement(tbl::Table, tempName::String, column::Symbol)
    complement = [c for c in keys(tbl.columns) if c != column]
    projection(tbl, tempName, complement...)
end
    
"Nested Loop Join Operator"
function join(tbl1::Table, tbl2::Table, on1::Symbol, on2::Symbol, tempName::String)
    @assert on1 in keys(tbl1.columns) && on2 in keys(tbl2.columns) 
    new_rows = Vector{String}[]
    key1 = tbl1.columns[on1]
    key2 = tbl2.columns[on2]
    for r1 in tbl1.rows
        for r2 in tbl2.rows
            if r1[key1] == r2[key2]
                push!(new_rows, vcat(r1, r2))
            end
        end
    end
    t1cols = sort_columns(tbl1.columns)
    t2cols = sort_columns(tbl2.columns)
    new_cols = Dict(c => i for (i, c) in enumerate(map(x -> x[1], vcat(t1cols, t2cols))))
    Table(tempName, new_cols, new_rows)
end
                     
"Selection Operator"
function selection(tbl::Table, tempName::String, column::Symbol, value)
    matches = []
    for row in tbl.rows
        if row[tbl.columns[column]] == value
            push!(matches, row)
        end
    end
    Table(tempName, tbl.columns, matches)
end

"Sort Operator"
function sortTableByKey(tbl::Table, tempName::String, key::Symbol; rev=false)
    function key_lt(x, y)
        x[tbl.columns[key]] < y[tbl.columns[key]]
    end
    Table(tempName, tbl.columns, sort(tbl.rows, lt=key_lt, rev=rev))
end
 
"Count Operator"
function count(tbl, tempName, key; newColumnName=:count)
    groups = unique(map(x -> x[tbl.columns[key]], tbl.rows))
    counts = Dict(g => 0 for g in groups)
    cols = Dict{Symbol, Int64}(key => 1, newColumnName => 2)
    for row in tbl.rows
        counts[row[tbl.columns[key]]] += 1
    end
    return Table(tempName, cols, [[g, c] for (g, c) in counts])
end


# end # module
