import argparse
import datetime
import logging
import getpass
import pandas as pd
import requests
from influxdb import DataFrameClient
from requests.auth import HTTPBasicAuth

logging.basicConfig(level="INFO", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

parser = argparse.ArgumentParser(description='Write connection stats from Netgear CG3100 to InfluxDB')
parser.add_argument('--hostname', required=False, type=str, default='192.168.100.1', help='Modem FQDN or IP-adress')
parser.add_argument('--username', required=False, type=str, default='admin', help='Modem username')
parser.add_argument('--password', required=False, type=str, help='Modem password')
parser.add_argument('--influx-host', required=False, type=str, default="localhost", help='InfluxDB FQDN or hostname')
parser.add_argument('--influx-port', required=False, type=int, default=8086, help='InfluxDB port')
parser.add_argument('--influx-username', required=False, type=str, help='InfluxDB username')
parser.add_argument('--influx-password', required=False, type=str, help='InfluxDB password')
args = parser.parse_args()

username = args.username

# If no password is set in arguments then prompt user for it
if args.password is None:
    password = getpass.getpass(username + " password: ")
else:
    password = args.password

try:
    r = requests.get('http://' + args.hostname + '/RgConnect.asp', auth=HTTPBasicAuth(username, password))
    r.raise_for_status()
    r.close()
except requests.exceptions.RequestException as e:
    raise SystemExit(e)


df_list = pd.read_html(r.text)

df_start_procedure = df_list[1]
df_down_ch = df_list[2]
df_up_ch = df_list[3]

datetime = datetime.datetime.utcnow().isoformat()

df_start_procedure = df_start_procedure.rename(columns=df_start_procedure.iloc[1]).drop(index=[0, 1]).reset_index(drop=True)
df_start_procedure["Datetime"] = datetime

df_down_ch = df_down_ch.rename(columns=df_down_ch.iloc[1]).drop(index=[0, 1]).reset_index(drop=True)
df_down_ch.rename(columns={'DOCSIS/EuroDOCSIS locked': "DOCSIS/EuroDOCSISLocked"}, inplace=True)
# Rename column because inconsistency between downstream and upstream
df_down_ch.rename(columns={'Symbol rate': "Symbol Rate"}, inplace=True)
df_down_ch[['Symbol Rate', 'Symbol Rate Unit']] = df_down_ch["Symbol Rate"].str.split(" ", expand=True)
df_down_ch[['Frequency', 'Frequency Unit']] = df_down_ch["Frequency"].str.split(" ", expand=True)
df_down_ch[['Power', 'Power Unit']] = df_down_ch["Power"].str.split(" ", expand=True)
df_down_ch[['SNR', 'SNR Unit']] = df_down_ch["SNR"].str.split(" ", expand=True)
df_down_ch["Direction"] = "Downstream"
df_down_ch["Datetime"] = datetime
df_down_ch[["Channel ID", "Symbol Rate", "Frequency", "Power", "SNR"]] = \
    df_down_ch[["Channel ID", "Symbol Rate", "Frequency", "Power", "SNR"]].apply(pd.to_numeric)

df_up_ch = df_up_ch.rename(columns=df_up_ch.iloc[1]).drop(index=[0, 1]).reset_index(drop=True)
df_up_ch[['Symbol Rate', 'Symbol Rate Unit']] = df_up_ch["Symbol Rate"].str.split(" ", expand=True)
df_up_ch[['Frequency', 'Frequency Unit']] = df_up_ch["Frequency"].str.split(" ", expand=True)
df_up_ch[['Power', 'Power Unit']] = df_up_ch["Power"].str.split(" ", expand=True)
df_up_ch["Direction"] = "Upstream"
df_up_ch["Datetime"] = datetime
# Rewrite rate without prefix
df_up_ch['Symbol Rate'] = df_up_ch['Symbol Rate'].astype(int) * 1000
df_up_ch['Symbol Rate Unit'] = "sym/sec"
df_up_ch[["Channel ID", "Symbol Rate", "Frequency", "Power"]] = \
    df_up_ch[["Channel ID", "Symbol Rate", "Frequency", "Power"]].apply(pd.to_numeric)

# Set Datetime index
df_start_procedure['Datetime'] = pd.to_datetime(df_start_procedure['Datetime'])
df_start_procedure = df_start_procedure.set_index('Datetime')
df_down_ch['Datetime'] = pd.to_datetime(df_down_ch['Datetime'])
df_down_ch = df_down_ch.set_index('Datetime')
df_up_ch['Datetime'] = pd.to_datetime(df_up_ch['Datetime'])
df_up_ch = df_up_ch.set_index('Datetime')

# Remove spaces in column headers
df_start_procedure.columns = df_start_procedure.columns.str.replace(' ', '')
df_down_ch.columns = df_down_ch.columns.str.replace(' ', '')
df_up_ch.columns = df_up_ch.columns.str.replace(' ', '')

df_start_procedure_datatags = ['Procedure']
df_start_procedure_fields = ['Status', 'Comment']

df_down_datatags = ['LockStatus', 'Modulation', 'ChannelID', 'DOCSIS/EuroDOCSISLocked', 'Direction']
df_down_fields = ['SymbolRate', 'Power', 'Frequency', 'SNR']

df_up_datatags = ['LockStatus', 'Modulation', 'ChannelID', 'Direction']
df_up_fields = ['SymbolRate', 'Power', 'Frequency']


if args.influx_username and args.influx_password:
    client = DataFrameClient(host=args.influx_host, port=args.influx_port,
                             username=args.influx_username, password=args.influx_password)
else:
    client = DataFrameClient(host=args.influx_host, port=args.influx_port)

client.write_points(
    df_down_ch,
    'statistics',
    tag_columns=df_down_datatags,
    field_columns=df_down_fields,
    database="cg3100",
    protocol="line"
)

client.write_points(
    df_up_ch,
    'statistics',
    tag_columns=df_up_datatags,
    field_columns=df_up_fields,
    database="cg3100",
    protocol="line"
)

client.write_points(
    df_start_procedure,
    'status',
    tag_columns=df_start_procedure_datatags,
    field_columns=df_start_procedure_fields,
    database="cg3100",
    protocol="line"
)

client.close()
