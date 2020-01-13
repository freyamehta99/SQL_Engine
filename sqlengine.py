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


def print_data(final_table, cols):
    num_print = len(cols)
    x = len(final_table['info'])
    j = 0
    ap = 0
    for i in range(x):
        if i == cols[j]:
            if ap != num_print - 1:
                print(final_table['info'][i] + ',', end=' ')
            else:
                print(final_table['info'][i], end=' ')
            j = j + 1
            ap = ap + 1
        if j == len(cols):
            break
    print()

    n = len(final_table['table'])
    ap = 0
    for i in range(n):
        num_attr = len(final_table['table'][i])
        k = 0
        ap = 0
        for j in range(num_attr):
            if j == cols[k]:
                if ap != num_print - 1:
                    print(final_table['table'][i][j] + ',', end=' ')
                else:
                    print(final_table['table'][i][j], end=' ')
                k = k + 1
                ap = ap + 1
            if k == len(cols):
                break
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


def distict_func(table, cols):
    n = len(table['table'])
    req = []
    for i in range(n):
        x = []
        for c in cols:
            x.append(table['table'][i][c])
        req.append(x)
    unique_data = [list(x) for x in set(tuple(x) for x in req)]

    num_print = len(cols)
    x = len(table['info'])
    j = 0
    ap = 0
    for i in range(x):
        if i == cols[j]:
            if ap != num_print - 1:
                print(table['info'][i] + ',', end=' ')
            else:
                print(table['info'][i], end=' ')
            j = j + 1
            ap = ap + 1
        if j == len(cols):
            break
    print()

    n = len(unique_data)
    ap = 0
    for i in range(n):
        num_attr = len(unique_data[i])
        ap = 0
        for j in range(num_attr):
            if ap != num_print - 1:
                print(unique_data[i][j] + ',', end=' ')
            else:
                print(unique_data[i][j], end=' ')
            ap = ap + 1
        print()


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

    if sum_flag == False and avg_flag == False and max_flag == False and min_flag == False and distinct_flag == False and where_flag == False:
        print_data(final_table, cols)

    elif sum_flag == True:
        print(sum_func(final_table['table'], cols[0]))
    elif avg_flag == True:
        print(avg_func(final_table['table'], cols[0]))
    elif max_flag == True:
        print(max_func(final_table['table'], cols[0]))
    elif min_flag == True:
        print(min_func(final_table['table'], cols[0]))
    elif distinct_flag == True:
        distict_func(final_table, cols)
    elif where_flag == True:
        return final_table, cols

    return final_table, cols


def process_condition(condition, final_table, cols_print):
    ans = []
    if '!=' in condition:
        val = int(condition.split('!=')[1])
        var = condition.split('!=')[0].strip()
        if '.' in var:
            t = var.split('.')[0]
            c = var.split('.')[1]
            var = t + '.' + c.upper()
        else:
            for t in metadata:
                if var in metadata[t]:
                    var = t + '.' + var.upper()
        print(var, val)
        col = -1
        for h in range(len(final_table['info'])):
            if var == final_table['info'][h]:
                col = h
                break
        for i in range(len(final_table['table'])):
            temp = []
            if int(final_table['table'][i][col]) != val:
                for c in cols_print:
                    temp.append(final_table['table'][i][c])
                ans.append(temp)
        return ans

    if '>=' in condition:
        val = int(condition.split('>=')[1])
        var = condition.split('>=')[0].strip()
        if '.' in var:
            t = var.split('.')[0]
            c = var.split('.')[1]
            var = t + '.' + c.upper()
        else:
            for t in metadata:
                if var in metadata[t]:
                    var = t + '.' + var.upper()
        col = -1
        for h in range(len(final_table['info'])):
            if var == final_table['info'][h]:
                col = h
                break
        for i in range(len(final_table['table'])):
            temp = []
            if int(final_table['table'][i][col]) >= val:
                for c in cols_print:
                    temp.append(final_table['table'][i][c])
                ans.append(temp)
        return ans

    if '>' in condition:
        val = int(condition.split('>')[1])
        var = condition.split('>')[0].strip()
        if '.' in var:
            t = var.split('.')[0]
            c = var.split('.')[1]
            var = t + '.' + c.upper()
        else:
            for t in metadata:
                if var in metadata[t]:
                    var = t + '.' + var.upper()
        col = -1
        for h in range(len(final_table['info'])):
            if var == final_table['info'][h]:
                col = h
                break
        for i in range(len(final_table['table'])):
            temp = []
            if int(final_table['table'][i][col]) > val:
                for c in cols_print:
                    temp.append(final_table['table'][i][c])
                ans.append(temp)
        return ans

    if '<=' in condition:
        val = int(condition.split('<=')[1])
        var = condition.split('<=')[0].strip()
        if '.' in var:
            t = var.split('.')[0]
            c = var.split('.')[1]
            var = t + '.' + c.upper()
        else:
            for t in metadata:
                if var in metadata[t]:
                    var = t + '.' + var.upper()
        col = -1
        for h in range(len(final_table['info'])):
            if var == final_table['info'][h]:
                col = h
                break
        for i in range(len(final_table['table'])):
            temp = []
            if int(final_table['table'][i][col]) <= val:
                for c in cols_print:
                    temp.append(final_table['table'][i][c])
                ans.append(temp)
        return ans

    if '<' in condition:
        val = int(condition.split('<')[1])
        var = condition.split('<')[0].strip()
        if '.' in var:
            t = var.split('.')[0]
            c = var.split('.')[1]
            var = t + '.' + c.upper()
        else:
            for t in metadata:
                if var in metadata[t]:
                    var = t + '.' + var.upper()
        col = -1
        for h in range(len(final_table['info'])):
            if var == final_table['info'][h]:
                col = h
                break
        for i in range(len(final_table['table'])):
            temp = []
            if int(final_table['table'][i][col]) < val:
                for c in cols_print:
                    temp.append(final_table['table'][i][c])
                ans.append(temp)
        return ans

    if '=' in condition:
        val = int(condition.split('=')[1])
        var = condition.split('=')[0].strip()
        if '.' in var:
            t = var.split('.')[0]
            c = var.split('.')[1]
            var = t + '.' + c.upper()
        else:
            for t in metadata:
                if var in metadata[t]:
                    var = t + '.' + var.upper()
        col = -1
        for h in range(len(final_table['info'])):
            if var == final_table['info'][h]:
                col = h
                break
        for i in range(len(final_table['table'])):
            temp = []
            if int(final_table['table'][i][col]) == val:
                for c in cols_print:
                    temp.append(final_table['table'][i][c])
                ans.append(temp)

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
                                     0].strip(), final_table, cols_print)
            res2 = process_condition(condition.split('and')[
                                     1].strip(), final_table, cols_print)
            for row in res1:
                if row in res2:
                    final_ans.append(row)
        elif 'or' in condition:
            res1 = process_condition(condition.split('and')[
                                     0].strip(), final_table, cols_print)
            res2 = process_condition(condition.split('and')[
                                     1].strip(), final_table, cols_print)
            final_ans = res1 + res2
        else:
            final_ans = process_condition(condition, final_table, cols_print)

        num_print = len(cols_print)
        x = len(final_table['info'])
        j = 0
        ap = 0
        for i in range(x):
            if i == cols_print[j]:
                if ap != num_print - 1:
                    print(final_table['info'][i] + ',', end=' ')
                else:
                    print(final_table['info'][i], end=' ')
                j = j + 1
                ap = ap + 1
            if j == len(cols_print):
                break
        print()
        n = len(final_ans)
        ap = 0
        for i in range(n):
            num_attr = len(final_ans[i])
            ap = 0
            for j in range(num_attr):
                if ap != num_print - 1:
                    print(final_ans[i][j] + ',', end=' ')
                else:
                    print(final_ans[i][j], end=' ')
                ap = ap + 1
            print()

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
        print("Usage: python3 20171104.py 'query'")
        print("Bye")
        sys.exit()


if __name__ == '__main__':
    main()