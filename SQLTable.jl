# module SQLTable

# export Table, Database, initDB, getTable, getAllRows, getColumn, getRowsWhere

struct Table
    name::String
    columns::Dict{Symbol, Int64}
    rows::Vector{Vector{String}}
    
    Table() = new()
    "Construct Initial Table"
    function Table(name, rows)
        new(
            name, 
            Dict(Symbol(f) => i for (i, f) in enumerate(rows[1])), 
            map(r -> String.(r), rows[2:end])
        )
    end
    
    function Table(name, columns, rows)
        new(name,
            Dict(Symbol(f) => i for (i, f) in enumerate(columns)),
            rows)
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
    
    
function getColumnIter(tbl::Table, column::Symbol)
    columnvector = []
    for r in tbl.rows
        push!(columnvector, r[tbl.columns[column]])
    end
    return columnvector
end

                     

function selection(tbl::Table, tempName::String, column::Symbol, value)
    matches = []
    for row in tbl.rows
        if row[tbl.columns[column]] == value
            push!(matches, row)
        end
    end
    return matches
end

# end # module
