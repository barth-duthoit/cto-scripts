# Databricks notebook source
# Job/cluster env: DATABRICKS_SECRET_SCOPE + DATABRICKS_SECRET_KEY_NOTION_COOKIE, or NOTION_COOKIE.
# See deployment/databricks/DEPLOY.txt

# COMMAND ----------

# Install from your Repo checkout (edit path after cloning Git in Workspace → Repos).
%pip install /Workspace/Repos/REPLACE_WITH_YOUR_USER_OR_ORG/cto-scripts

# COMMAND ----------

from notion_ai_usage.databricks_job import main

raise SystemExit(main())
