module SQLTable

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
end

struct Database
    name::String
    tables::Dict{Symbol, Table}
    files::Vector{String}
end

"Initializes the database and all table structures"
function initDB(dbname)
    if !(dbname in readdir("databases\\"))
        error("Database $dbname does not exist")
    end

    dbfiles = readdir("databases\\$dbname")
    tables = Dict{Symbol, Table}()
    for f in dbfiles
        tblname = f[1:end-4]
        rowdata = split.(readlines(open("databases\\$dbname\\$f")), "|")
        tables[Symbol(tblname)] = Table(tblname, rowdata)
    end

    Database(dbname, tables, dbfiles)
end

getTable(db::Database, tblname::Symbol) = db.tables[tblname]
getAllRows(tbl::Table) = tbl.rows
getColumn(tbl::Table, column::Symbol) =  [r[tbl.columns[column]] for r in tbl.rows]
getRowsWhere(tbl, column, value) = filter(r -> r[tbl.columns[column]] == value, getAllRows(tbl))

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

function join(tbl1, tbl2, on)
    joined = []
    for r1 in tbl1
        for r2 in tbl2
            if r1[tbl1.columns[on]] == r2[tbl2.columns[on]]
                
end


end # module