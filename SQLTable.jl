# module SQLTable

# export Table, Database, initDB, getTable, getAllRows, getColumn, getRowsWhere

struct Table
    name::String
    columns::Dict{Any, Int64}
    rows::Vector{Vector{Any}}
    
    Table() = new()
    "Construct Initial Table"
    function Table(name, rows)
        new(
            name, 
            Dict(Symbol(f) => i for (i, f) in enumerate(rows[1])), 
            map(r -> String.(r), rows[2:end])
        )
    end
    
    function Table(name, columns::Vector{Symbol}, rows)
        new(name,
            Dict(Symbol(f) => i for (i, f) in enumerate(columns)),
            rows)
    end

    function Table(name::String, columns::Dict{Symbol, Int64}, rows::Vector{Vector{Any}})
        new(name, columns, rows)
    end
end

struct Database
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
        tblname = f[1:end-4]
        rowdata = split.(readlines(open("$dbstring$f")), "|")
        tables[Symbol(tblname)] = Table(tblname, rowdata)
    end
    
    Database(dbname, tables, dbfiles)
end

getTable(db::Database, tblname::Symbol) = db.tables[tblname]
getRows(tbl::Table) = tbl.rows
getColumns(tbl::Table) = tbl.columns
getColumn(tbl::Table, column::Symbol) = [r[tbl.columns[column]] for r in tbl.rows]
getRowsWhere(tbl, column, value) = filter(r -> r[tbl.columns[column]] == value, getAllRows(tbl))
dictToVector(d) = [[k, v] for (k,v) in d]
columns_lt(x, y) = x[2] < y[2]

function projection(tbl::Table, tempName::String, columns::Symbol...)
    if length(columns) == 1
        return Table(tempName, columns, getColumn(tbl, columns[1]))
    end
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

function projectComplement(tbl::Table, tempName::String, column::Symbol)
    complement = [c for c in keys(tbl.columns) if c != column]
    projection(tbl, tempName, complement...)
end
    
    
function getColumnIter(tbl::Table, column::Symbol)
    columnvector = []
    for r in tbl.rows
        push!(columnvector, r[tbl.columns[column]])
    end
    return columnvector
end

function join(tbl1::Table, tbl2::Table, on1::Symbol, on2::Symbol)
    @assert on1 in keys(tbl1.columns) && on2 in keys(tbl2.columns) 
    new_rows = Vector{String}[]
    for r1 in tbl1.rows
        for r2 in tbl2.rows
            if r1[tbl1.columns[on1]] == r2[tbl2.columns[on2]]
                push!(new_rows, vcat(r1, r2))
            end
        end
    end
    return new_rows
end
                     

function selection(tbl::Table, tempName::String, column::Symbol, value)
    matches = []
    for row in tbl.rows
        if row[tbl.columns[column]] == value
            push!(matches, row)
        end
    end
    return Table(tempName, tbl.columns, matches)
end


### Sorting

function sortTableByKey(tbl::Table, tempName::String, key::Symbol; rev=false)
    function key_lt(x, y)
        x[tbl.columns[key]] < y[tbl.columns[key]]
    end
    Table(tempName, tbl.columns, sort(tbl.rows, lt=key_lt, rev=rev))
end
    

function groupBy(tbl, tempName, key)
    groups = unique(map(x -> x[tbl.columns[key]], tbl.rows))
    counts = Dict(g => 0 for g in groups)
    cols = Dict{Symbol, Int64}(key => 1, :count => 2)
    for row in tbl.rows
        counts[row[tbl.columns[key]]] += 1
    end
    return Table(tempName, cols, [[g, c] for (g, c) in counts])
end



# end # module
