import os
import time
import paramiko
import ptool
from . import configure

try:
    C = configure.Config()
    C.init_from_user_config()
except FileNotFoundError:
    exit()


def get_ssh(site: str = None):
    """
    Creates a ssh connection object to remote host.
    `site` must be defined in the config file.
    
    Example:
    >>> import ptool.configure
    >>> C = ptool.configure.Config()
    >>> hostname = 'levante'
    >>> hostname in C
    True
    >>> 'undefined_hostname' in C
    False
    """
    host = C[site]['host']
    user = C[site]['user']
    identityfile = C.rc['ssh_keys'][site]
    assert host, "host value is required"
    assert user, "user value is required"
    assert identityfile, "path to identityfile is required"
    params = {}
    params["hostname"] = host
    params["username"] = user
    params["key_filename"] = identityfile
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(**params)
    return ssh


def ensure_checksum_script(ssh):
    "Ensures `checksums.py` script availability on remote machine"
    # TODO
    # Just checking the existance checksums.py file may not be sufficient
    # Ensuring the latest version of this file on the remote machine is desired
    #
    csfile = "checksums.py"
    cspath = os.path.join(os.path.dirname(ptool.__file__), csfile)
    i, o, e = ssh.exec_command("ls ~")
    for line in o.read().decode().splitlines():
        if csfile == line:
            break
    else:
        with ssh.open_sftp() as scp:
            scp.put(cspath, csfile)


def get_checksum(site: str, pool: str):
    """Computes checksums for the given `poolname` on remote host

    `site`: host-name as defined in the config file (example: levante)
    `pool`: pool-name as defined in the config file (example: fesom2)

    Returns
    """
    pyexe = C[site]['python_executable']
    host = C[site]['host']
    _pool = C[site]['pool'][pool]
    path = _pool['path']
    outfile = _pool['outfile']
    ignore = ",".join(_pool.get("ignore"))
    print(f"found config for {pool} on {host}")
    print(f"    pyexec={pyexe}")
    print(f"    path={path}")
    print(f"    outfile={outfile}")
    print(f"    ignore={ignore}")
    print(f"creating ssh connection to {site}")
    stime = time.time()
    ssh = get_ssh(site)
    ensure_checksum_script(ssh)
    i, o, e = ssh.exec_command("echo $HOME")
    homedir = o.read().decode().strip().rstrip("/")
    OUTFILE = outfile.replace("~", homedir)
    cmd = f"{pyexe} checksums.py {path} --outfile {OUTFILE} --ignore {ignore}"
    print(f"calculating checksum for pool {pool}")
    i, o, e = ssh.exec_command(cmd, get_pty=True)
    for line in iter(lambda: o.read(512).decode("utf-8", "ignore"), ""):
        print(line, end="")
    rel_outfile = outfile.replace("~/", "./")
    with ssh.open_sftp() as scp:
        scp.get(OUTFILE, rel_outfile)
    print(f"checksum file: {rel_outfile}")
    ssh.close()
    etime = time.time()
    print(f"Elapsed: {etime-stime}s")
    return
