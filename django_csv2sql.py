#!/usr/bin/python3.2
import sys
import argparse
import csv
import sqlite3

class CSV2SQL:
    def __init__(self, csv_file, field_num, sql_file, table_name):
        self.csv_file = csv_file
        self.table_name = table_name
        self.field_num = field_num
        self.conn = sqlite3.connect(sql_file)
        self.curs = self.conn.cursor()

    def insert_rows(self):
        plc_hldrs = "?, " * self.field_num
        plc_hldrs = "(" + plc_hldrs[:-2] + ")"
        sql_stmt = "insert into " + self.table_name + " values " + plc_hldrs
        with open(self.csv_file, 'r', encoding='utf-8') as csv_raw:
            self.reader = csv.reader(csv_raw)
            for row in self.reader:
                values = row[1:self.field_num]
                values.insert(0, None)
                values = tuple(values)
                self.curs.execute(sql_stmt, values)
        self.conn.commit()

    def close_db(self):
        self.conn.close()

def main():
    parser = argparse.ArgumentParser(description='Process csv file')
    parser.add_argument('csv_file', type=str, help='source csv file')
    parser.add_argument('num_fields', type=int, help='number of fields in csv row')
    parser.add_argument('sql_file', type=str, help='target sql file')
    parser.add_argument('table_name', type=str, help='sql table to insert csv rows')
    args = parser.parse_args()
    converter = CSV2SQL(args.csv_file, args.num_fields,
                        args.sql_file, args.table_name)
    converter.insert_rows()
    converter.close_db()

if __name__ == "__main__":
    main()
