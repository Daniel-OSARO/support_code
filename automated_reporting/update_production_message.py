import asyncio,argparse,datetime,ast
from ssh_exec import ssh_exec
from box import Box
import time,json
from slack_post_message import api_call_slack,api_upload_slack,channel_id,find_latest_production_message,translate_mentions
from ssh_remote_pnpvers import query_pnp_version_string,write_day_shift_string
from coupang_hosts import hosts

def parse_args():
  parser = argparse.ArgumentParser(
    description="edits production message (latest)"
  )
  parser.add_argument('--crews',type=str,help='label on who are on the shift', default='')
  parser.add_argument('--update',type=str,help='JSON of {cell_num:"quoted status update string"}')
  parser.add_argument('--backup',type=int,default=[],nargs='+',help='specify cell numbers for standby during shift')
  parser.add_argument('--maintenance',type=int,default=[],nargs='+',help='specify cell numbers for maintenance (down) during shift')
  args = parser.parse_args()
  return args.crews,args.update,args.backup,args.maintenance



if __name__=='__main__':
  crews,update_stats_str,cellnum_backup,cellnum_maintenance=parse_args()
  update_dict=ast.literal_eval(update_stats_str)
  old_message=find_latest_production_message()
  if old_message is None:
    raise ValueError('no production message made by script today...')
  old_ts=old_message.ts
  new_message=write_day_shift_string(hosts,crews,cellnum_backup,cellnum_maintenance,update_dict)
  api_call_slack('chat.update',channel=channel_id,ts=old_ts,text=translate_mentions(new_message))
