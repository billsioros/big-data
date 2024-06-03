#!/bin/bash
USER=osboxes

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
scp -r ~/.ssh/ $USER@slave:~/
