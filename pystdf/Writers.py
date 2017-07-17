#
# PySTDF - The Pythonic STDF Parser
# Copyright (C) 2006 Casey Marshall
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

import sys, os
from time import strftime, localtime
from xml.sax.saxutils import quoteattr
from pystdf import V4

import pdb
import sqlite3

def format_by_type(value, field_type):
    if field_type in ('B1', 'N1'):
        return '%02X' % (value)
    else:
        return str(value)

class AtdfWriter:
    
    @staticmethod
    def atdf_format(rectype, field_index, value):
        field_type = rectype.fieldStdfTypes[field_index]
        if value is None:
            return ""
        elif rectype is V4.gdr:
            return '|'.join([str(v) for v in value])
        elif field_type[0] == 'k': # An Array of some other type
            return ','.join([format_by_type(v, field_type[2:]) for v in value])
        elif rectype is V4.mir or rectype is V4.mrr:
            field_name = rectype.fieldNames[field_index]
            if field_name.endswith('_T'): # A Date-Time in an MIR/MRR
                return strftime('%H:%M:%S %d-%b-%Y', localtime(value))
            else:
                return str(value)
        else:
            return str(value)
    
    def __init__(self, stream=sys.stdout):
        self.stream = stream
    
    def after_send(self, dataSource, data):
        line = '%s:%s%s' % (data[0].__class__.__name__,
            '|'.join([self.atdf_format(data[0], i, val) for i, val in enumerate(data[1])]), '\n')
        self.stream.write(line)
    
    def after_complete(self, dataSource):
        self.stream.flush()

class XmlWriter:
    extra_entities = {'\0': ''}
    
    @staticmethod
    def xml_format(rectype, field_index, value):
        field_type = rectype.fieldStdfTypes[field_index]
        if value is None:
            return ""
        elif rectype is V4.gdr:
            return ';'.join([str(v) for v in value])
        elif field_type[0] == 'k': # An Array of some other type
            return ','.join([format_by_type(v, field_type[2:]) for v in value])
        elif rectype is V4.mir or rectype is V4.mrr:
            field_name = rectype.fieldNames[field_index]
            if field_name.endswith('_T'): # A Date-Time in an MIR/MRR
                return strftime('%H:%M:%ST%d-%b-%Y', localtime(value))
            else:
                return str(value)
        else:
            return str(value)
    
    def __init__(self, stream=sys.stdout):
        self.stream = stream
    
    def before_begin(self, dataSource):
        self.stream.write('<Stdf>\n')
    
    def after_send(self, dataSource, data):
        self.stream.write('<%s' % (data[0].__class__.__name__))
        for i, val in enumerate(data[1]):
            fmtval = self.xml_format(data[0], i, val)
            self.stream.write(' %s=%s' % (data[0].fieldNames[i], quoteattr(fmtval, self.extra_entities))) 
        self.stream.write('/>\n')
    
    def after_complete(self, dataSource):
        self.stream.write('</Stdf>\n')
        self.stream.flush()

class CsvWriter:
    extra_entities = {'\0': ''}
    
    @staticmethod
    def csv_format(rectype, field_index, value):
        field_type = rectype.fieldStdfTypes[field_index]
        if value is None:
            retval = ""
        elif rectype is V4.gdr:
            retval = ';'.join([str(v) for v in value])
        elif field_type[0] == 'k': # An Array of some other type
            retval = ','.join([format_by_type(v, field_type[2:]) for v in value])
        elif rectype is V4.mir or rectype is V4.mrr:
            field_name = rectype.fieldNames[field_index]
            if field_name.endswith('_T'): # A Date-Time in an MIR/MRR
                retval = strftime('%H:%M:%ST%d-%b-%Y', localtime(value))
            else:
                retval = str(value)
        else:
            retval = str(value)
        return retval
    
    def __init__(self, stream=sys.stdout):
        self.stream = stream
        self.conn = sqlite3.connect(':memory:')
        self.scale_dict = dict()
        self.scale_dict[0] = ''
        self.scale_dict[3] = 'm'
        self.scale_dict[6] = 'u'
        self.scale_dict[9] = 'n'
        self.scale_dict[12] = 'p'
        self.scale_dict[-3] = 'k'
        self.scale_dict[-6] = 'M'
        self.scale_dict[-9] = 'G'
        self.scale_dict[-12] = 'T'	

    def sql_format(self, stdf_type):
        """ """
        if stdf_type.endswith('_T'):
            return "TEXT"
        elif stdf_type.startswith(("B", "I", "U")):
            return "INTEGER"
        elif stdf_type.startswith(("k", "R")):
            return "REAL"
        elif stdf_type.startswith("Cn"):
            return "TEXT"

    def units_format(self, scaling):
        return self.scale_dict[scaling]

    def add_record(self, record):
        """
        Add a record so we can display it however we'd like
        """
        c = self.conn.cursor()
        rectype = record[0]
        fields = []
        vals = []            

        # if rectype in [V4.mpr, V4.ptr, V4.ftr]:
        #     # Per part test info
        #     tablename = rectype.__class__.__name__
        # else:
        
        if rectype is V4.pir:
            self.part.append(self.part[-1]+1)
        elif rectype is V4.wir:
            self.wafer.append(self.wafer[-1]+1)
        elif rectype is V4.mir:
            self.lot.append(self.lot[-1]+1)
        tablename = rectype.__class__.__name__

        fields.append('LOT_INDEX')
        vals.append(str(self.lot[-1]))
        fields.append('WAFER_INDEX')
        vals.append(str(self.wafer[-1]))
        fields.append('PART_INDEX')
        vals.append(str(self.part[-1]))

        for i, val in enumerate(record[1]):
            field_type = self.sql_format(rectype.fieldStdfTypes[i])
            # fields.append("{} {}".format(str(rectype.fieldNames[i]), str(field_type)))
            fields.append("{}".format(str(rectype.fieldNames[i])))
            vals.append(str(val))
        try:
            c.execute("CREATE TABLE IF NOT EXISTS {} {};".format(tablename, str(tuple(fields)) ) )
        except:
            # Table already exists
            print "Table {} already exists: {}".format(tablename, tuple(fields))

        try:
            c.execute("INSERT INTO {} VALUES {};".format(tablename, tuple(vals)))
        except:
            print "Could not INSERT data {} into {}".format(tuple(vals), tablename)
        self.conn.commit()

    def before_begin(self, dataSource):
    	self.wafer = [0]
        self.part = [0]
        self.lot = [0]
    
    def after_send(self, dataSource, data):
        self.add_record(data)
    
    def after_complete(self, dataSource):
        debug = 0;
        c = self.conn.cursor()

        if debug:
            # List the name of each table (record type)        
            res = c.execute("SELECT name FROM sqlite_master WHERE type='table';")
            for name in res:
                self.stream.write('{},'.format(name[0]))
            self.stream.write('\n')

            # List the parameters for each test
            res = c.execute("SELECT * FROM Mpr LIMIT 5;")
            col_list = [str(x[0]) for x in res.description]
            self.stream.write(', '.join(col_list))
            self.stream.write('\n')

        # Get the list of tests
        res = c.execute("SELECT TEST_TXT FROM Mpr WHERE PART_INDEX='1';")
        hdr = ['PART_INDEX', 'TEST_T', 'NUM_TEST', 'WAFER_INDEX', 'LOT_INDEX']
        emptyhdr = " , "*(len(hdr)-1)
        emptyhdr = emptyhdr.split(',')
        tests = hdr + [str(x[0]) for x in list(res)]
        self.stream.write(', '.join(tests))
        self.stream.write('\n')

        # Get the list of test numbers
        res = c.execute("SELECT TEST_NUM FROM Mpr WHERE PART_INDEX='1'")
        tests = ['Test #'] + emptyhdr[1:] + list(res)
        self.stream.write(', '.join(str(x[0]) for x in tests))
        self.stream.write('\n')

        # Get the list of units
        res = c.execute("SELECT UNITS, RES_SCAL FROM Mpr WHERE PART_INDEX='1'")
        tests = ['Units'] + emptyhdr[1:] + [self.units_format(int(x[1])) + str(x[0]) for x in list(res)]
        self.stream.write(', '.join(tests))
        self.stream.write('\n')

        # Get the high end limits
        res = c.execute("SELECT HI_LIMIT, HLM_SCAL FROM Mpr WHERE PART_INDEX='1'")
        tests = ['High limit'] + emptyhdr[1:] + [str(float(x[0])*10**(int(x[1]))) for x in list(res)]
        self.stream.write(', '.join(tests))
        self.stream.write('\n')

        # Get the low end limits
        res = c.execute("SELECT LO_LIMIT, LLM_SCAL FROM Mpr WHERE PART_INDEX='1'")
        tests = ['Low limit'] + emptyhdr[1:] + [str(float(x[0])*10**(int(x[1]))) for x in list(res)]
        self.stream.write(', '.join(tests))
        self.stream.write('\n')

        # Get per part results
        # try:
            # for name in res:
            # for row in c.execute("SELECT * FROM {}".format(str('Mpr'))):
        for pidx in self.part:
            for widx in self.wafer:
                for lidx in self.lot:
                    # Get the test time and number of tests for the part
                    test_time='unknown'
                    num_tests='unknown'
                    parms = ['TEST_T', 'NUM_TEST']
                    res = c.execute("SELECT {} FROM {} WHERE PART_INDEX='{}' AND WAFER_INDEX='{}' AND LOT_INDEX='{}';".format(",".join(parms), 
                        str('Prr'), str(pidx), str(widx), str(lidx)))
                    for row in list(res):
                        test_time = str(row[0])
                        num_tests = str(row[1])

                    # Get the actual results
                    parms = ['RTN_RSLT', 'RES_SCAL', 'RSLT_CNT']
                    res = c.execute("SELECT {} FROM {} WHERE PART_INDEX='{}' AND WAFER_INDEX='{}' AND LOT_INDEX='{}';".format(",".join(parms), 
                        str('Mpr'), str(pidx), str(widx), str(lidx)))
                    # res = str(list(res)).replace("[", " ")
                    # res = res.replace("]", " ")
                    outstr = []
                    for row in list(res):
                        if debug:
                            self.stream.write('{}\n'.format(row))  # For debug
                        raw = row[0].strip('[]').split(',')[0]
                        scaled = float(raw)*(10**int(row[1]))
                        outstr.append(str(scaled))
                    if outstr:
                        self.stream.write(','.join([str(pidx),test_time,num_tests,str(widx),str(lidx)] + outstr))
                        self.stream.write('\n')
        # except:
        #     self.stream.write('Exception!\n')
        self.conn.close()
        self.stream.flush()