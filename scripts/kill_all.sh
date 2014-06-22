#!/bin/bash

# script kills all processes with Synergy in its name

ps aux | grep Synergy | grep -v grep | awk '{print$2}' | xargs kill
