# module SQLTable

export Table, Database, initDB, getTable, getAllRows, getColumn, getRowsWhere

struct Table
    name::String
    columns::Dict{Symbol, Int64}
    rows::Vector{Vector{String}}
    
    Table() = new()
    function Table(name, rows)
        new(
            name, 
            Dict(Symbol(f) => i for (i, f) in enumerate(rows[1])), 
            map(r -> String.(r), rows[2:end])
        )
    end
    
    function Table(name, columns, rows)
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
getAllRows(tbl::Table) = tbl.rows
getColumn(tbl::Table, column::Symbol) =  [r[tbl.columns[column]] for r in tbl.rows]
getRowsWhere(tbl, column, value) = filter(r -> r[tbl.columns[column]] == value, getAllRows(tbl))

function getColumns(tbl::Table, columns::Symbol...)
    if length(columns) == 1
        return getColumn(tbl, columns[1])
    end
    newheader = [columns[1]]
    newvalues = getColumn(tbl, columns[1])
    for column in columns[2:end]
        push!(newheader, column)
        newvalues = hcat(newvalues, getColumn(tbl, column))
    end
    newrows = [newvalues[i, :] for i in 1:size(newvalues)[1]]
    
    Table("tempName",
          Dict(h => i for (i, h) in enumerate(newheader)),
          newvalues)
end
    
    
function getColumnIter(tbl::Table, column::Symbol)
    columnvector = []
    for r in tbl.rows
        push!(columnvector, r[tbl.columns[column]])
    end
    return columnvector
end 

function getRowsWhereIter(tbl, column, value)
    matches = []
    for row in tbl.rows
        if row[tbl.columns[column]] == value
            push!(matches, row)
        end
    end
    return matches
end

function writeTempTable(tbl)
    
end
#=
function join(tbl1, tbl2, on)
    joined = []
    for r1 in tbl1
        for r2 in tbl2
            if r1[tbl1.columns[on]] == r2[tbl2.columns[on]]

end
=#

# end # module
