# module ParseQuery

# using ..SQLTable, ..Operators

# export readQuery, parseQuery

function readQuery(f)
    readlines(open(f))
end

function parseQuery(db, query; newColumnName=:counted_ssn)
    tempTable = Table()
    for line in query
        line = filter(c -> c != ',', line)
        parts = split(lowercase(strip(line)), " ")
        operator = parts[1]
        tbl1 = "$(parts[2]).csv" in db.files ? getTable(db, Symbol(parts[2])) : readBarCSV(db, "$(parts[2]).csv")
        tempName = String(parts[end])
        if operator == "selection"
            key, value = split(parts[3], "=")
            tempTable = selection(tbl1, tempName, Symbol(key), String(value))
            
        elseif operator == "projection"
            cols = Symbol.(parts[3:end-1])
            tempTable = projection(tbl1, tempName, cols...)
            
        elseif operator == "count"
            key = Symbol(parts[3])
            tempTable = count(tbl1, tempName, key, newColumnName=newColumnName)
            
        elseif operator == "join"
            tbl2 = "$(parts[3]).csv" in db.files ? getTable(db, Symbol(parts[3])) : readBarCSV(dv, "$(parts[3]).csv")
            on1, on2 = split(parts[4], "=")
            tempTable = join(tbl1, tbl2, Symbol(on1), Symbol(on2), tempName)
            
        elseif operator == "sort"
            key = Symbol(parts[3])
            tempTable = sortTableByKey(tbl1, tempName, key)

        end
        displayTable(tempTable)
        writeBarCSV(db, tempTable)
    end
    return tempTable
end

# end # module
