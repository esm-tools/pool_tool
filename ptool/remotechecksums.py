import os
import time
import paramiko
import ptool
from . import conf


def get_ssh(
    name: str = None, host: str = None, user: str = None, identityfile: str = None
):
    ssh_config = paramiko.SSHConfig()
    if name:
        with open(os.path.expanduser("~/.ssh/config")) as fid:
            ssh_config.parse(fid)
        c = ssh_config.lookup(name)
        assert "identityfile" in c, "Valid entry requires identityfile"
    else:
        assert host, "host value is required"
        assert user, "user value is required"
        assert identityfile, "path to identityfile is required"
        txt = f"""Host {host}
        User {user}
        IdentityFile {identityfile}
        """
        sc = ssh_config.from_text(txt)
        c = sc.lookup(host)
    C = {}
    C["hostname"] = c["hostname"]
    C["username"] = c["user"]
    C["key_filename"] = c["identityfile"]
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(**C)
    return ssh


def ensure_checksum_script(ssh):
    csfile = "checksums.py"
    cspath = os.path.join(os.path.dirname(ptool.__file__), csfile)
    i, o, e = ssh.exec_command("ls ~")
    for line in o.read().decode().splitlines():
        if csfile == line:
            break
    else:
        with ssh.open_sftp() as scp:
            scp.put(cspath, csfile)


def get_checksum(host, pool, identityfile):
    stime = time.time()
    c = conf.get(host)
    pyexec = c.get("python_executable")
    user, hostname = c.get("host").split("@")
    cc = c.get("pool").get(pool)
    path = cc.get("path")
    outfile = cc.get("outfile")
    ignore = ",".join(cc.get("ignore"))
    print(f"found config for {pool} on {host}")
    print(f"    pyexec={pyexec}")
    print(f"    path={path}")
    print(f"    outfile={outfile}")
    print(f"    ignore={ignore}")
    print(f"creating ssh connection to {hostname}")
    ssh = get_ssh(host=hostname, user=user, identityfile=identityfile)
    ensure_checksum_script(ssh)
    i, o, e = ssh.exec_command("echo $HOME")
    homedir = o.read().decode().strip().rstrip("/")
    OUTFILE = outfile.replace("~", homedir)
    # cmd = f"~/cs/bin/python -m checksums {path} --outfile {OUTFILE}"
    cmd = f"{pyexec} checksums.py {path} --outfile {OUTFILE} --ignore {ignore}"
    print(f"calculating checksum for pool {pool}")
    i, o, e = ssh.exec_command(cmd)
    for line in o:
        print(line.strip())
    rel_outfile = outfile.replace("~/", "./")
    with ssh.open_sftp() as scp:
        scp.get(OUTFILE, rel_outfile)
    print(f"checksum file: {rel_outfile}")
    ssh.close()
    etime = time.time()
    print(f"Elapsed: {etime-stime}s")
    return
