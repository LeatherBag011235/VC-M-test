Dear reviewer,

Befor you launch this app, please, make sure to config your environment with vcm_env.yml file. It contains the list of sufficient dependancies.

If you like to see how backend goes, please, call run.py with "python -m src.run" comand from repo directory, python's module importing system is most tricky.
To launch Streamlit itself, run "streamlit run src/front.py" from repo directory.

This app was developed on Linux, but presumably should run on Windows as well.

What is this app capable of?
- Parse and display:
  - YC company name
  - Website
  - Description
  - Link to YC page
  - Link to LinkedIn page
- Refresh data on demand
- Show small summary of refresh operation

What is this app not capable of?
- Crawlinf Linkedin pages and search for "YC S25" tag. Linkedin is extrimely hostile to vpn users. Tbh I failed even to sign in myself. To my mind, this task require vpn with residential exit or somthing, cause even full browser clinet spoofing didn't help.
- Searching other companies on Linkedin with "YC S25" tag. Same reason.

Highlits:
- I found non-official API that provides most of required fields (the only missing was Linkedin links). It refreshes every day.
- I use multiprocessing for scraping Linkedin links from companies' pages, capped at 20 processes.
- Neve hit rate limit even with 31 process
