import sys
import os
import csv
import re
import sqlparse

metadata = {}


def get_tables(filename):
    try:
        with open(filename) as f:
            new_table = False
            table_name = ''
            attribute = []
            for line in f.readlines():
                line = line.strip()
                if line == '<begin_table>':
                    new_table = True
                    table_name = ''
                    attribute = []
                elif line == '<end_table>':
                    metadata[table_name] = attribute[:]
                elif new_table:
                    new_table = False
                    table_name = line.lower()
                else:
                    attribute.append(line.lower())
    except:
        print("Can't access metadata")
        pass


def validate_query(query):
    if bool(re.match('^select.*from.*', query)) == False:
        return False
    else:
        return True


def print_data(final_table_info, final_table_data, cols, all):
    num_print = len(cols)
    x = len(final_table_info)
    j = 0
    ap = 0
    for i in range(x):
        if i == cols[j]:
            if ap != num_print - 1:
                print(final_table_info[i] + ',', end=' ')
            else:
                print(final_table_info[i], end=' ')
            j = j + 1
            ap = ap + 1
        if j == len(cols):
            break
    print()

    n = len(final_table_data)
    ap = 0
    for i in range(n):
        num_attr = len(final_table_data[i])
        k = 0
        ap = 0
        if all == False:
            for j in range(num_attr):
                if j == cols[k]:
                    if ap != num_print - 1:
                        print(final_table_data[i][j] + ',', end=' ')
                    else:
                        print(final_table_data[i][j], end=' ')
                    k = k + 1
                    ap = ap + 1
                if k == len(cols):
                    break
            print()
        else:
            for j in range(num_attr):
                if ap != num_print - 1:
                    print(final_table_data[i][j] + ',', end=' ')
                else:
                    print(final_table_data[i][j], end=' ')
                ap = ap + 1
            print()


def sum_func(arr, idx):
    sum = 0
    n = len(arr)
    for i in range(n):
        sum = sum + int(arr[i][idx])
    return sum


def avg_func(arr, idx):
    sum = 0
    n = len(arr)
    for i in range(n):
        sum = sum + int(arr[i][idx])
    return sum / n


def max_func(arr, idx):
    max = -1000000000
    n = len(arr)
    for i in range(n):
        if max < int(arr[i][idx]):
            max = int(arr[i][idx])
    return max


def min_func(arr, idx):
    min = 1000000000
    n = len(arr)
    for i in range(n):
        if min > int(arr[i][idx]):
            min = int(arr[i][idx])
    return min


def distinct_func(table_info, table_data, cols):
    n = len(table_data)
    req = []
    for i in range(n):
        x = []
        for c in cols:
            x.append(table_data[i][c])
        req.append(x)
    unique_data = [list(x) for x in set(tuple(x) for x in req)]

    print_data(table_info, unique_data, cols, True)


def join(table, tname):
    path = 'files/' + tname + '.csv'
    t = []
    final_table = []
    with open(path, 'r') as f:
        csvreader = csv.reader(f)
        for row in csvreader:
            t.append(row)
    for r1 in table:
        for r2 in t:
            final_table.append(r1 + r2)
    return final_table


def create_table(tables, columns, star_flag, sum_flag, avg_flag, max_flag, min_flag, distinct_flag, where_flag):
    final_table = {}
    final_table['info'] = []
    final_table['table'] = []

    for t in tables:
        for c in metadata[t]:
            final_table['info'].append(t + "." + c.upper())
    path1 = 'files/' + tables[0] + '.csv'
    with open(path1, 'r') as f:
        csvreader = csv.reader(f)
        for row in csvreader:
            final_table['table'].append(row)
    for i in range(1, len(tables)):
        final_table['table'] = join(final_table['table'], tables[i])

    cols = []
    if star_flag == True:
        for i in range(len(final_table['info'])):
            cols.append(i)
    else:
        for i in range(len(final_table['info'])):
            for j in range(len(columns)):
                if final_table['info'][i] == columns[j]:
                    cols.append(i)

    if where_flag == False:
        if sum_flag == False and avg_flag == False and max_flag == False and min_flag == False and distinct_flag == False:
            print_data(final_table['info'], final_table['table'], cols, False)
        elif sum_flag == True:
            print(sum_func(final_table['table'], cols[0]))
        elif avg_flag == True:
            print(avg_func(final_table['table'], cols[0]))
        elif max_flag == True:
            print(max_func(final_table['table'], cols[0]))
        elif min_flag == True:
            print(min_func(final_table['table'], cols[0]))
        elif distinct_flag == True:
            distinct_func(final_table['info'], final_table['table'], cols)
    else:
        return final_table, cols

    return final_table, cols


def compute_condition(query, operator, final_table, tables):
    var = query.split(operator)[0].strip()
    val = int(query.split(operator)[1])
    if '.' in var:
        t = var.split('.')[0]
        c = var.split('.')[1]
        var = t + '.' + c.upper()
    else:
        if len(tables) == 1:
            var = tables[0] + '.' + var.upper()
        else:
            for t in metadata:
                if var in metadata[t]:
                    var = t + '.' + var.upper()
    col = -1
    for h in range(len(final_table['info'])):
        if var == final_table['info'][h]:
            col = h
            break
    return col, val


def process_condition(condition, final_table, cols_print, tables):
    ans = []
    if '!=' in condition:
        col, val = compute_condition(condition, '!=', final_table, tables)
        for i in range(len(final_table['table'])):
            temp = []
            if int(final_table['table'][i][col]) != val:
                for c in cols_print:
                    temp.append(final_table['table'][i][c])
                ans.append(temp)
        return ans

    if '>=' in condition:
        col, val = compute_condition(condition, '>=', final_table, tables)
        for i in range(len(final_table['table'])):
            temp = []
            if int(final_table['table'][i][col]) >= val:
                for c in cols_print:
                    temp.append(final_table['table'][i][c])
                ans.append(temp)
        return ans

    if '>' in condition:
        col, val = compute_condition(condition, '>', final_table, tables)
        for i in range(len(final_table['table'])):
            temp = []
            if int(final_table['table'][i][col]) > val:
                for c in cols_print:
                    temp.append(final_table['table'][i][c])
                ans.append(temp)
        return ans

    if '<=' in condition:
        col, val = compute_condition(condition, '<=', final_table, tables)
        for i in range(len(final_table['table'])):
            temp = []
            if int(final_table['table'][i][col]) <= val:
                for c in cols_print:
                    temp.append(final_table['table'][i][c])
                ans.append(temp)
        return ans

    if '<' in condition:
        col, val = compute_condition(condition, '<', final_table, tables)
        for i in range(len(final_table['table'])):
            temp = []
            if int(final_table['table'][i][col]) < val:
                for c in cols_print:
                    temp.append(final_table['table'][i][c])
                ans.append(temp)
        return ans

    if '=' in condition:
        col, val = compute_condition(condition, '=', final_table, tables)
        for i in range(len(final_table['table'])):
            temp = []
            if int(final_table['table'][i][col]) == val:
                for c in cols_print:
                    temp.append(final_table['table'][i][c])
                ans.append(temp)
        return ans

    return ans


def process_query(query):
    if validate_query(query) == False:
        print("Invalid Query")
        print("Format: Select * from table_name")
        sys.exit()

    sum_flag = avg_flag = max_flag = min_flag = False
    distinct_flag = False
    new_query = query.strip("select").strip()
    columns = new_query.split("from")[0].strip()
    if bool(re.match('^(sum)\(.*\)', columns)):
        sum_flag = True
        columns = columns.replace('sum', '').strip().strip('()')
    if bool(re.match('^(avg)\(.*\)', columns)):
        avg_flag = True
        columns = columns.replace('avg', '').strip().strip('()')
    if bool(re.match('^(max)\(.*\)', columns)):
        max_flag = True
        columns = columns.replace('max', '').strip().strip('()')
    if bool(re.match('^(min)\(.*\)', columns)):
        min_flag = True
        columns = columns.replace('min', '').strip().strip('()')
    if bool(re.match('^distinct.*', columns)):
        distinct_flag = True
        columns = columns.replace('distinct', '').strip()
    columns = columns.split(',')
    for i in range(len(columns)):
        columns[i] = columns[i].strip()

    new_query = new_query.split("from")[1].strip()
    tables = new_query.split("where")[0].split(',')

    for i in range(len(tables)):
        tables[i] = tables[i].strip()
        if tables[i] not in metadata:
            print("Table does not exist")
            sys.exit()

    star_flag = False
    if len(columns) == 1 and columns[0] == '*':
        star_flag = True

    if star_flag == False:
        if len(tables) == 1:
            for i in range(len(columns)):
                if '.' in columns[i]:
                    f = columns[i].split('.')[1]
                    if tables[0] not in metadata:
                        print("Table does not exist")
                        sys.exit()
                    if f not in metadata[tables[0]]:
                        print("Invalid Attribute")
                        sys.exit()
                    columns[i] = tables[0] + '.' + f.upper()
                else:
                    if columns[i] not in metadata[tables[0]]:
                        print("Invalid Attribute")
                        sys.exit()
                    columns[i] = tables[0] + '.' + columns[i].upper()
        else:
            for i in range(len(columns)):
                if '.' in columns[i]:
                    t = columns[i].split('.')[0]
                    f = columns[i].split('.')[1]
                    if t not in metadata:
                        print("Table does not exist")
                        sys.exit()
                    if f not in metadata[t]:
                        print("Invalid Attribute")
                        sys.exit()
                    columns[i] = t + '.' + f.upper()
                else:
                    flag = 1
                    for t in metadata:
                        if columns[i] in metadata[t]:
                            columns[i] = t + '.' + columns[i].upper()
                            flag = 1
                            break
                        else:
                            flag = 0
                    if flag == 0:
                        print("Invalid Attribute")
                        sys.exit()

    where_flag = False
    if bool(re.match('^select.*from.*where.*', query)):
        where_flag = True
        final_table = {}
        final_ans = []
        final_table, cols_print = create_table(tables, columns, star_flag, sum_flag,
                                               avg_flag, max_flag, min_flag, distinct_flag, where_flag)
        condition = new_query.split("where")[1].strip()
        if 'and' in condition:
            res1 = process_condition(condition.split('and')[
                                     0].strip(), final_table, cols_print, tables)
            res2 = process_condition(condition.split('and')[
                                     1].strip(), final_table, cols_print, tables)
            for row in res1:
                if row in res2:
                    final_ans.append(row)
        elif 'or' in condition:
            res1 = process_condition(condition.split('or')[
                                     0].strip(), final_table, cols_print, tables)
            res2 = process_condition(condition.split('or')[
                                     1].strip(), final_table, cols_print, tables)
            for row in res1:
                final_ans.append(row)
            for row in res2:
                if row not in final_ans:
                    final_ans.append(row)
        else:
            final_ans = process_condition(
                condition, final_table, cols_print, tables)

        if len(final_ans) > 0:
            if sum_flag == True:
                sum = sum_func(final_ans, 0)
                print(sum)
            elif avg_flag == True:
                avg = avg_func(final_ans, 0)
                print(avg)
            elif max_flag == True:
                max = max_func(final_ans, 0)
                print(max)
            elif min_flag == True:
                min = min_func(final_ans, 0)
                print(min)
            elif distinct_flag == True:
                distinct_func(final_table['info'], final_ans, cols_print)
            else:
                print_data(final_table['info'], final_ans, cols_print, True)
        else:
            print("No row matches the given condition")

    else:
        create_table(tables, columns, star_flag, sum_flag,
                     avg_flag, max_flag, min_flag, distinct_flag, where_flag)


def main():
    get_tables('files/metadata.txt')
    if len(sys.argv) >= 2:
        query = sys.argv[1]
        if query[-1] != ';':
            print("Semicolon missing")
            sys.exit()
        query = query[:-1].strip()
        process_query(query.lower())
    else:
        print("Usage: python3 sqlengine.py 'query'")
        print("Bye")
        sys.exit()


if __name__ == '__main__':
    main()