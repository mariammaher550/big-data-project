#!/bin/bash
scp -P 2222 scripts/streamlit_app.py root@localhost:/root/
ssh -p 2222 root@localhost 'sudo fuser -k 60000/tcp' # free the port before using it
ssh -p 2222 root@localhost 'streamlit run streamlit_app.py --server.port 60000'