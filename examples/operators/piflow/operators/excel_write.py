"""XLSX workbook as an output byte stream, without destination paths or append mode."""
import datetime
import io
import re
from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.utils.cell import coordinate_to_tuple
from dacp_piflow_support import params, MAX_BYTES


def process(ctx):
    p = params(ctx, optional=['header', 'dataAddress'])
    header, address = p.get('header', True), p.get('dataAddress', 'Sheet1!A1')
    if type(header) is not bool or not isinstance(address, str):
        raise ValueError('header must be boolean; dataAddress must be a string')
    match = re.fullmatch(r"(?:(?:'((?:[^']|'')+)'|([^!]+))!)?([A-Za-z]+[1-9][0-9]*)", address)
    if not match:
        raise ValueError('dataAddress must be [Sheet!]A1 start cell; bounded write ranges are not supported')
    sheet = (match[1] or match[2] or 'Sheet1').replace("''", "'")
    row_start, col_start = coordinate_to_tuple(match[3])
    if row_start>100000 or col_start>16384:
        raise ValueError('start cell outside supported bounds')
    source = ctx.input('source')
    names = source.schema.names
    if col_start+len(names)>16385:
        raise ValueError('too many columns for XLSX')
    wb = Workbook(write_only=True)
    ws = wb.create_sheet(sheet)
    size, count = 0, row_start-1
    for _ in range(row_start-1):
        ws.append([])
    def append(values):
        nonlocal count
        count += 1
        if count>1000000:
            raise ValueError('XLSX output exceeds one million rows')
        cells = [None]*(col_start-1)
        for value in values:
            if isinstance(value, (dict,list,bytes)):
                raise ValueError('XLSX requires scalar nonbinary values')
            if isinstance(value, (datetime.date,datetime.time,datetime.datetime)):
                value=value.isoformat()
            cell=WriteOnlyCell(ws,value=value)
            if isinstance(value,str):
                cell.data_type='s'  # preserve text, including leading =; never create formulas
            cells.append(cell)
        ws.append(cells)
    try:
        if header:
            append(names)
        for batch in source.records():
            size+=batch.nbytes
            if size>MAX_BYTES:
                raise ValueError('XLSX input exceeds 128 MiB')
            for row in batch.to_pylist():
                append([row[n] for n in names])
        buffer=io.BytesIO()
        wb.save(buffer)
        out=ctx.bytes_output('result','xlsx')
        buffer.seek(0)
        while chunk:=buffer.read(65536):
            out.write(chunk)
    finally:
        wb.close()


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)
