import sqlite3
import sys
import os.path

class PacketDB:
    eth2_tbl = ["dst_mac text","src_mac text","ether_type text"]

    ip_tbl = ["version text","ihl int","service_type text","total_len int",
              "ip_id text","flags text","frag_off text","ttl text",
              "proto text","header_checksum text","src_addr text","dest_addr text"]

    tcp_tbl = ["src_port int","dest_port int","sequence_num int",
               "acknowledgment_num int","data_offset text","control_bits text",
               "window int","checksum text","urg_ptr text"]

    def __init__(self, packets, name='pkts.db'):
        self.pkts = packets
        self.pkt_num = len(self.pkts)
        self.proto_fields = {'eth2':self.eth2_tbl, 'ip':self.ip_tbl, 'tcp':self.tcp_tbl}
        self.conn = sqlite3.connect(name)
        self.curs = self.conn.cursor()

    def create_table(self, table):
        if table in self.proto_fields:
            crt_tbl_stmt = 'create table %s (id integer primary key, %s)'
            fields = reduce(lambda a,x: a + ', ' + x, self.proto_fields[table])
            sql_stmt = crt_tbl_stmt % (table, fields)
            self.curs.execute(sql_stmt)
        else:
            raise Exception("Not a valid protocol")

    def table_exist(self, table):
         result = self.curs.execute("select name from sqlite_master where type = 'table'").fetchall()
         if len(result) == 0:
             return False
         else:
             return self._table_in_result(result, table)

    def _table_in_result(self, result, table):
        for tbl in result:
            if table in tbl[0]:
                return True
            else:
                continue
        return False

    def insert_pkts(self):
        """
        precondition: packets are valid
        """
        while len(self.pkts) != 0:
            pkt = self.pkts.pop()
            pkt_layers = self.separate_pkt_into_layers(pkt)
            self.insert_layers_into_db(pkt_layers)
        self.conn.commit()

    def insert_layers_into_db(self, pkt_layers):
        for table in ['tcp', 'ip', 'eth2']:
            if not self.table_exist(table):
                self.create_table(table)
            layer_fields = self.separate_layers_into_fields(pkt_layers, table)
            sql_stmt="insert into " + table + " values (null, " + (len(layer_fields) -1) * "?," + "?)"
            self.curs.execute(sql_stmt, layer_fields)

    def separate_layers_into_fields(self, pkt_layers, table):
        layers = pkt_layers[table]
        fields = []
        for field in layers:
            fields.append(field.split(', ')[1])
        return tuple(fields)

    def separate_pkt_into_layers(self, pkt):
        layer = {'eth2': [], 'ip': [], 'tcp': []}
        layer_flag = ''
        for field in pkt.split('",'):
            if field.split(',')[0][1:] == "dest_mac":
                layer_flag = 'eth2'
            elif field.split(',')[0][1:] == "version":
                layer_flag = 'ip'
            elif field.split(',')[0][1:] == "src_port":
                layer_flag = 'tcp'
            elif field == '':
                continue
            layer[layer_flag].append(field[1:])
        return layer

    def close_connection(self):
        self.conn.close()

def main():
    # read in packets in chunks using python generators
    packets_raw = sys.stdin.read()
    packets = packets_raw.split('\n')
    packets = packets[:len(packets)-1]
    pkt_db = PacketDB(packets)
    pkt_db.insert_pkts()
    pkt_db.close_connection()

if __name__ == "__main__":
    main()
