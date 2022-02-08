#!/usr/bin/env python3
import sqlite3
import sys
import gzip
import os
import re


def drop_table(db_fname, table_name):
    con = sqlite3.connect(db_fname)
    cur = con.cursor()
    cur.execute("DROP TABLE %s" % table_name)
    cur.close()
    con.close()


def auto_table_name(table_fname):
    table_name = os.path.basename(table_fname)
    table_name = re.sub('.gz$','', table_name)
    table_name = re.sub('.bgz$','', table_name)
    table_name = re.sub('.txt$', '', table_name)
    table_name = re.sub('.tsv$', '', table_name)

    return table_name


def auto_coltype(all_cols):
    types = []
    seen = []
    for cols in all_cols:
        while len(types) < len(cols):
            types.append("integer")
            seen.append(False)
        
        for i, col in enumerate(cols):
            if not col:
                continue
            seen[i] = True
            if types[i] == 'integer':
                if col:
                    try:
                        v = int(col)
                    except:
                        types[i] = 'real'
            if types[i] == 'real':
                if col:
                    try:
                        v = float(col)
                    except:
                        types[i] = 'text'

    return types

def create_table(cur, name, headers, coltypes):
    sql = 'CREATE TABLE %s (' % name
    for i, coltype in enumerate(coltypes):
        if headers and len(headers) > i:
            colname = headers[i]
        else:
            colname = 'col_%s' % (i+1)
        if (i>0):
            sql +=', '
        sql += "'%s' %s" % (colname, coltype)
    sql += ')'
    print(sql)
    cur.execute(sql)

def insert_line(cur, table_name, coltypes, cols):
    sql = 'INSERT INTO %s VALUES(' % table_name
    vals = []
    for i, col in enumerate(cols):
        if i>0:
            sql += ', '
        sql += '?'

        if col:
            if coltypes[i] == 'integer':
                try:
                    val = int(col)
                except:
                    val = col
            elif coltypes[i] == 'float':
                try:
                    val = float(col)
                except:
                    val = col
            else:
                val = col
        else:
            val = ""
        
        vals.append(val)

    sql += ')'

    #print((sql, vals))
    cur.execute(sql, vals)


def import_table(db_fname, table_fname, table_name=None, header=False, header_comment=False, bufsize=20):
    if not os.path.exists(db_fname):
        sys.stderr.write("Creating SQLite database: %s\n" % (db_fname))

    con = sqlite3.connect(db_fname)
    cur = con.cursor()

    if not table_name:
        if table_fname == '-':
            table_name = 'stdin'
        else:
            table_name = auto_table_name(table_fname)

    print("Importing file: %s => %s" % (table_fname, table_name))

    if table_fname == '-':
        f = sys.stdin
    else:
        f = open(table_fname, 'rb')
        if f.read(2) == b'\x1f\x8b':
            f.close()
            f = gzip.open(table_fname, 'rt')
        else:
            f.close()
            f = open(table_fname, 'rt')

    headers = None
    coltypes = None
    
    buf = []
    inbuf = True
    inheader = (header or header_comment)

    last_line = ""
    for line in f:
        if line[0] == '#':
            last_line=line
            continue

        if inheader and (header or header_comment):
            if header_comment:
                headers = last_line[1:].strip('\r\n').split('\t')
            else:
                headers = line.strip('\r\n').split('\t')
            
            inheader = False
            if not header_comment:
                continue


        cols = line.strip('\r\n').split('\t')

        if inbuf:
            buf.append(cols)
            if len(buf) >= bufsize:
                inbuf = False
                coltypes = auto_coltype(buf)
                create_table(cur, table_name, headers, coltypes)
                for cols in buf:
                    while len(cols) < len(coltypes):
                        cols.append("")
                    insert_line(cur, table_name, coltypes, cols)
        else:
            while len(cols) < len(coltypes):
                cols.append("")
            insert_line(cur, table_name, coltypes, cols)

    if inbuf:
        coltypes = auto_coltype(buf)
        create_table(cur, headers, coltypes)
        for cols in buf:
            while len(cols) < len(coltypes):
                cols.append("")
            insert_line(cur, table_name, coltypes, cols)

    cur.close()
    con.commit()
    con.close()

    if table_fname != '-':
        f.close()


def err(msg=''):
    if msg:
        sys.stderr.write('%s\n\n' % msg)

    sys.stderr.write("""\
Usage: tab-to-sqlite.py import [--header] [--header-comment] [-t name] db.sqlite table.txt
       tab-to-sqlite.py rm db.sqlite table_name
""")
    sys.exit(1)


if __name__ == '__main__':
    cmd = None
    db_fname = None
    table_fname = None
    table_name = None
    header = False
    header_comment = False

    last = None

    for arg in sys.argv[1:]:
        if not cmd:
            cmd = arg.lower()
            if not cmd in ["import", "rm", "drop"]:
                err("Unknown command: %s" % arg)
            continue
        
        if cmd in ['rm', 'drop']:
            if not db_fname:
                db_fname = arg
            elif not table_name:
                table_name = arg
        elif cmd == 'import':
            if last == '-t':
                table_name = arg
                last = None
            elif arg in ['-t']:
                last = arg
            elif arg == '--header':
                header = True
            elif arg == '--header-comment':
                header_comment = True
                header = True
            elif not db_fname:
                db_fname = arg
            elif not table_fname:
                if os.path.exists(arg) or arg == "-":
                    table_fname = arg
                else:
                    err("Missing input tab file: %s" % (arg))
            else:
                err("Only one table can be imported at a time")

    if not db_fname:
        err("Missing database filename")

    if cmd == 'rm':
        drop_table(db_fname, table_name)
    if cmd == 'import':
        import_table(db_fname, table_fname, table_name, header, header_comment)

