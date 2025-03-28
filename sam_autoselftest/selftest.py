#!python3
# python3 selftest.py --crews '@sam'

import asyncio,argparse,datetime
from ssh_exec import ssh_exec
from box import Box
import time,json
from slack_post_message import api_post_slack,api_upload_slack
from ssh_remote_pnpvers import query_pnp_version_string,write_day_shift_string
from summarized_ST_results import extract_information as local_selftest_summary
def selftest_start(host):
  stdout1,stderr1=ssh_exec(host,r"curl  -sSX POST 'http://localhost:51061/v1/pnp/self-test'")
  if stderr1:
    raise RuntimeError(stderr1)
  return Box(json.loads(stdout1))

def start_selftest_batched(hosts):
  boxes=[selftest_start(host) for host in hosts]
  return boxes


async def wait_until_complete(host,action_id,label=''):
  poll_command=f"curl -sSX GET 'http://localhost:51061/v1/pnp/self-test/{action_id}'"
  def poll():
    poll_text,poll_err=ssh_exec(host,poll_command)
    if poll_err: raise RuntimeError(poll_err)
    return Box(json.loads(poll_text))
  poll_result=poll()
  while True:
    if poll_result.status=='ERROR':
      if label!='':
        print(f'❌ {label} FAILED')
      break
    if poll_result.status != 'COMPLETE':
      await asyncio.sleep(5)
      poll_result=poll()
    else:
      if label!='':
        print(f'✅ {label} SUCCESS')
      break
  return poll_result

def send_selftest_result_to_slack(labels,results,actionIds,hosts,post_only_passed_cells,thread_ts=None):
  for label,result,action_id,host in zip(labels,results,actionIds,hosts):
    result_literal=local_selftest_summary(label,result)
    if post_only_passed_cells and result.status!='COMPLETE':
      continue

    filename=f'C{label}ST'
    if thread_ts is None:
      api_upload_slack(json.dumps(result),filename,result_literal)
    else:
      api_upload_slack(json.dumps(result),filename,result_literal,thread_ts=thread_ts)

def selftest_post_versions_and_results(labels,hosts,results,actionIds,post_only_passed_cells,crews,cellnum_backup,cellnum_maintenance):
  parent_post_content=write_day_shift_string(hosts,crews,cellnum_backup,cellnum_maintenance)
  parent_post_detail=api_post_slack(parent_post_content)
  parent_post_ts=parent_post_detail.ts
  send_selftest_result_to_slack(labels,results,actionIds,hosts,post_only_passed_cells,parent_post_ts)

def remote_summary(host,action_id):
  stdout,stderr=ssh_exec(host,f'python3 /home/admin/script/summarized_ST_results.py {action_id}')
  if stderr:
    raise RuntimeError(stderr)
  else:
    return stdout


async def selftest_coupang(cell_number_selection,post_only_passed_cells,crews,cellnum_backup,cellnum_maintenance):
  from coupang_hosts import hosts
  filtered_hosts=[host for k,host in enumerate(hosts) if (k+1) in cell_number_selection]
  actionID_boxes=start_selftest_batched(filtered_hosts)

  cell_labels=[str(u) for u in cell_number_selection]

  for lb,box1 in zip(cell_labels,actionID_boxes):
    try:
      print(f'  {lb}: {box1.actionId}')
    except:
      print(box1)

  actionIds=[box.actionId for box in actionID_boxes]
  results=await asyncio.gather(*[wait_until_complete(host,box.actionId,cell_label) for (host,box,cell_label) in zip(filtered_hosts,actionID_boxes,cell_labels)])
  selftest_post_versions_and_results(cell_labels,filtered_hosts,results,actionIds,post_only_passed_cells,crews,cellnum_backup,cellnum_maintenance)
  post_shift_mots()
  
def post_shift_mots():
  for line in generate_production_mots():
    api_post_slack(line)




def generate_production_mots():
  # Get the current date and time
  now = datetime.datetime.now()

  # Determine whether it's "day" or "night" based on the time
  shift = "night" if now.hour >= 17 else "day"

  # Format the current date as MM/DD/YY
  date_str = now.strftime("%m/%d/%y")

  # Generate the production lines
  production_lines = [f"C{i} {shift} production {date_str}" for i in range(1, 8)]
  return production_lines
  
def parse_inargs():
  parser = argparse.ArgumentParser(
    description="Triggers self test on coupang cells"
  )

  # Define positional arguments
  parser.add_argument(
    'numbers',
    metavar='N',
    type=int,
    nargs='*',
    help="A sequence of cell to process",
    default=[1,2,3,4,5,6,7]
  )



  # Define optional arguments
  parser.add_argument(
    '--skip-fail',
    action='store_true',
    help="Won't post to slack for failed cells. Allows site crew to retry selftest"
  )

  parser.add_argument('--crews',type=str,help='label on who are on the shift', default='')
  parser.add_argument('--backup',type=int,default=[],nargs='+',help='specify cell numbers for standby during shift')
  parser.add_argument('--maintenance',type=int,default=[],nargs='+',help='specify cell numbers for maintenance (down) during shift')
  
  # Parse the arguments
  args = parser.parse_args()
  return args.numbers,args.skip_fail,args.crews,args.backup,args.maintenance

if __name__=='__main__':
  arguments=parse_inargs()
  print('➡️  triggering selftest for all coupang cells...')
  result=asyncio.run(selftest_coupang(*arguments))

  
  
