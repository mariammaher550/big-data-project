#!/bin/bash
sudo fuser -k 60000/tcp # free the port before using it
streamlit run scripts/streamlit_app.py --server.port 60000