module Operators

using ..SQLTable

export projection, projectComplement, join, selection, sortTableByKey, count

"Projection Operator"
function projection(tbl::Table, tempName::String, columns::Symbol...)
    if length(columns) == 1
        col = columns[1]
        return Table(tempName, columns, getColumn(tbl, col))
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
                r2 = [r2[i] for i in 1:length(r2) if i != key2]
                push!(new_rows, vcat(r1, r2))
            end
        end
    end
    t1cols = sort_columns(tbl1.columns)
    t2cols = [e for e in sort_columns(tbl2.columns) if e[1] != on2]
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


end # module
