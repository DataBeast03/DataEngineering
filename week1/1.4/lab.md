Lab
===

**Repeat the exercise from class, but this time use StarCluster.**

Add the `ipcluster` plugin if you haven't already.

Near the bottom of `.starcluster/config`:
```bash
######################
## Built-in Plugins ##
######################
# The following plugins ship with StarCluster and should work out-of-the-box.
# Uncomment as needed. Don't forget to update your PLUGINS list!
# See http://star.mit.edu/cluster/docs/latest/plugins for plugin details.
# .
# .
# .
[plugin ipcluster]
SETUP_CLASS = starcluster.plugins.ipcluster.IPCluster
# Enable the IPython notebook server (optional)
ENABLE_NOTEBOOK = True
# Set a password for the notebook for increased security
# This is optional but *highly* recommended
NOTEBOOK_PASSWD = a-secret-password

[plugin pypkginstaller]
setup_class = starcluster.plugins.pypkginstaller.PyPkgInstaller
packages = ipython, sklearn
```

Set `CLUSTER_SIZE` to `3` for more memory *(see [aws.amazon.com/ec2/instance-types](http://aws.amazon.com/ec2/instance-types/) for details)*:
```bash
[cluster smallcluster]
# number of ec2 instances to launch
CLUSTER_SIZE = 3
NODE_IMAGE_ID = ami-6b211202
PLUGINS = pypkginstaller, ipcluster
SPOT_BID = 0.10
```
Also set `SPOT_BID` to `0.10` (or less?) to save \$\$\$ *(see [aws.amazon.com/ec2/purchasing-options/spot-instances](http://aws.amazon.com/ec2/purchasing-options/spot-instances/) for details)*

Start your new cluster:

`$ starcluster start my_cluster`

Copy your credentials to your cluster:

`$ starcluster put my_cluster --user sgeadmin ~/Downloads/credentials.csv /home/sgeadmin/`

This should, as a side effect, add your cluster to the list of known hosts on your machine. 
In my experience, it often doesn't, however. 
Therefore, **you will want to:**

```bash
starcluster sshmaster my_cluster
```
**before you do the following (or `Client` will hang forever):**

```python
from os.path import expanduser
url_file = expanduser('~/.starcluster/ipcluster/SecurityGroup:@sc-my_medium_cluster-us-east-1.json')
sshkey = expanduser('~/.ssh/mykey.pem')  # replace with your pem
client = Client(url_file, sshkey = sshkey)
```

Check to see how many engines you have running:

```python
dview = client.direct_view()
len(client.ids)
```

```bash
starcluster put my_medium_cluster --user sgeadmin digits_cv_00* /mnt/sgeadmin/
```

We need to copy the files from the ephemeral drive (*i.e* `/mnt/`) on the host node to each of the other nodes. 
*e.g.*
```
%%px -t0
%%bash
scp /mnt/sgeadmin/digits_cv_00* node001:/mnt/sgeadmin/
scp /mnt/sgeadmin/digits_cv_00* node002:/mnt/sgeadmin/
```
You will also want to create a new list of filenames:

```python
remote_filenames = ['/mnt/sgeadmin/' + filename.split('/')[-1] for filename in digits_split_filenames]
```