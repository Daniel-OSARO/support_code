from ssh_exec import ssh_exec
from coupang_hosts import hosts
import re
from slack_post_message import api_post_slack
import datetime
def query_pnp_version_string(host):
  output,error=ssh_exec(host,r'docker ps | grep pnp$')
  if error:
    raise RuntimeError(error)
  information=re.split('\\s\\s+',output)
  return information[1].replace('osaroai/vidarr-rs:','')

def query_pnp_versions(hosts):
  versions={index+1:query_pnp_version_string(host) for index,host in enumerate(hosts)}
  return versions

def write_day_shift_string(hosts,homer,cellnum_backup=[],cellnum_maintenance=[],custom_statuses={}):
  versions=query_pnp_versions(hosts)
  now=datetime.datetime.now()
  shift='Day' if now.hour<18 else 'Night'
  datestring=now.strftime(r'%A %b %d')
  def determine_production_plan_label(cell):
    if cell in cellnum_backup:
      return 'Production (Backup) ⌛️ '
    elif cell in cellnum_maintenance:
      return 'Maintenance 🛠️ '
    elif cell in custom_statuses:
      return custom_statuses[cell]
    else:
      return 'Production ✅ '
  message=f''' {datestring} ({shift} shift) {homer}
Production plan:

'''+'\n'.join(
    f'> Cell{cell} - `{version}` - '+determine_production_plan_label(cell) for cell,version in versions.items()
  )
  return message

  
if __name__=='__main__':
  api_post_slack(write_day_shift_string(hosts,'@sam 🥡'))
  pass
