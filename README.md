# bmc-ctm-2-helix-integration

## tpl

Beta version of the TPL for discovering Control-M jobs and folders into DSM.

Usage steps:
1) Import TPLs
2) Add to outpost Control-M API Credentials
3) Add to outpost SSH/windows credentials for hosts running Control-M/Server, EM, and agents (if not already configured)
4) Run discovery scan against hosts running Control-M Server, EM, and agents. (TPLs only trigger if at least one EM and one server software instances are found and modeled)
5) Search for "Control-M Job". If any were discovered and modeled they'll be under "Deployment".
6) Open visualization for any job.
7) Change "Display" option to "All directly connected nodes->Directly Connected"

Jobs (Deployments) will be connected to Collections: Folders, subfolders, applications, and subapplications.

Jobs may also be connected to Control-M/Server and Agent responsible for it.

Jobs may also be connected to connection profiles.


## blueprints

Model Control-M workflows as a service using different starting points:
- Using Application attribute of jobs and folders
- Using Subapplication attribute of jobs and folders
- From an initial Folder (+ 2 subfolders)
- From an SLA Service job
- Control-M Server to all jobs it orquestrates

## event_classes

Used by Control-M alert engine scripts.

### Control Alert Engine Scripts

Recommended the usage of:

https://github.com/dcompane/controlm_toolset/tree/main/sendAlarmToScript/Python/BHOM

There is also an official version in the public Control-M repo, but it doesn't provide enough information to events for BHOM to uniquely lookup the alerting job in DSM:

https://github.com/controlm/automation-api-community-solutions/blob/master/helix-control-m/2-external-monitoring-tools-examples/alerts-to-bhom/README.md


## event_policies

Event policy that deduplicates CTM events and does DSM node lookup refinements.

## monitor_policies

Create by SEAL team for collecting metrics from Control-M using the script KM to query Control-M APIs

## dashboards

Also by SEAL Team.

Helix Dashboards that leverage metrics collected by the monitor_policies for a Control-M Workflow Insights experience inside Helix.

