import glob
import os
import questionary
import subprocess
import fnmatch
from ruamel.yaml import YAML

BASEPATH_SCRIPTS = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH_DEFAULT = f"{BASEPATH_SCRIPTS}/config.yaml"


class Config(dict):
    """
    A dictionary subclass to help the user to configure ptool and creating their own
    ``config.yaml``
    """

    def __init__(self):
        """
        Instantiates the Config object. Initialize the path variables, the rc info
        and loads the default ``config.yaml`` distributed with the package.
        """
        # Path attributes
        self.default_path = CONFIG_PATH_DEFAULT
        self.user_config_path = None
        self.ptoolrc_path = os.path.abspath(os.path.expanduser("~/.ptoolrc.yaml"))
        self.current_directory = os.getcwd()
        # rc dictionary initialization
        self.rc = {}
        # Loads the default ``config.yaml`` into ``self``
        self.load_config()

    def load_config(self, path=None):
        """
        Loads the dictionary stored in ``path`` (default in ``self.default_path``)
        into ``self``. Uses ``ruamel-yaml`` to keep comments once ``self`` is dump
        into a yaml in other methods.

        Input
        -----
        path : str
            Path to the default yaml file to be loaded
        """
        if not path:
            path = self.default_path

        with open(path, "r") as f:
            yaml = YAML()
            config = yaml.load(f)

        super().__init__(config)

    def save(self, file="config"):
        """
        Saves the given yaml ``file``.

        Input
        -----
        file : str
            The possible values for ``file`` are:
            * ``config``: (default) saves the ``self`` dictionary into the
                ``self.user_config_path`` path
            * ``rc``: saves the ``self.rc`` value into the ``self.ptoolrc_path``
                path
        """
        if file == "config":
            file_path = self.user_config_path
            content = self
        elif file == "rc":
            file_path = self.ptoolrc_path
            content = self.rc

        if not file_path:
            print(f"ERROR: no {file} defined yet")
            exit(1)

        with open(file_path, "w") as f:
            yaml = YAML()
            yaml.default_flow_style = False
            yaml.dump(dict(content), f)

    def create_rc(self):
        """
        Creates the rc file (``~/.ptoolrc.yaml``), a file that contains information from
        the user's configuration of the tool. Initializes the rc variables in
        ``self.rc``. ``self.rc`` contains the following key-values:
        * ``user_config_path``: the path to the user's ``config.yaml``
        * ``machines``: a list of configured machines
        * ``ssh_keys``: a dictionary which keys correspond to the names of the machines
            and the values to the path of the corresponding ssh-keys
        """
        if os.path.isfile(self.ptoolrc_path):
            rewrite_rc = questionary.confirm(
                f"{self.ptoolrc_path} already exists. Do you want to overwrite it?"
            ).ask()
            if not rewrite_rc:
                print("Config operation stopped by the user")
                exit(0)
            self.load_rc()

        if not "user_config_path" in self.rc:
            self.rc["user_config_path"] = self.user_config_path

        if not "machines" in self.rc:
            self.rc["machines"] = []

        if not "ssh_keys" in self.rc:
            self.rc["ssh_keys"] = {}

        self.save(file="rc")

    def load_rc(self):
        """
        Loads the ``~/.ptoolrc.yaml`` file into ``self.rc``
        """
        if not os.path.isfile(self.ptoolrc_path):
            print(
                "ERROR: Cannot find ~/.ptoolrc.yaml. Please, make sure you configure "
                "ptool before running the current command. To configure ptool use "
                "``ptool config``"
            )

        with open(self.ptoolrc_path, "r") as f:
            yaml = YAML()
            self.rc.update(yaml.load(f))

    def init_from_user_config(self):
        """
        Reads the rc file and loads the user ``config.yaml`` based on the location in
        ``self.rc["user_config_path"]``. For methods that are not run from ``self.all``.
        """
        if not self.user_config_path:
            self.load_rc()
            self.user_config_path = self.rc["user_config_path"]
            self.load_config(path=self.user_config_path)

    def create_user_config(self, verbose=True):
        """
        Completes config stored in ``self`` with user-specific information, using
        questionaries to ask the user about the missing info interactively, and
        prepares the ``~/.ptoolrc.yaml`` file. The steps involved in this method are:
        - Ask the user where should the ``config.yaml`` be created
        - Load the user ``config.yaml`` if it already exists
        - Store the ``config.yaml`` path in the rc file
        - Find which machines need to be configured
        - Ask for the user name in the different machines
        - Save the new information into the user ``config.yaml``

        Return
        ------
        machines : list
            List of machines that were configured
        """
        # Ask the user where should the ``config.yaml`` be created
        path = questionary.path(
            "Enter the directory in which to save the config.yaml "
            f"({self.current_directory}): ",
        ).ask()

        if not path:
            path = self.current_directory

        path = os.path.abspath(os.path.expanduser(path))

        # Check if the directory exist
        if not os.path.isdir(path):
            print(f"ERROR: {path} does not exist or it's not a directory")
            exit(1)

        # Check if the path exists
        self.user_config_path = f"{self.current_directory}/config.yaml"
        if os.path.isfile(self.user_config_path):
            rewrite_config = questionary.confirm(
                f"The file {self.user_config_path} already exists. Do you want to "
                "overwrite it?"
            ).ask()
            if rewrite_config:
                # Load the user ``config.yaml`` if it already exists
                self.load_config(self.user_config_path)
            else:
                print("Config operation stopped by the user")
                exit(0)
        elif verbose:
            print(path)

        # Store config location in rc
        self.rc["user_config_path"] = self.user_config_path
        self.save(file="rc")

        # Ask about which machines need to be configured
        machines = questionary.checkbox(
            "Which machines would you like to configure?", choices=self.keys()
        ).ask()

        for machine in machines:
            # Check user
            if "user" in self[machine]:
                print(f"User already defined for {machine}: {self[machine]['user']}")
            else:
                self[machine]["user"] = questionary.text(
                    f"Specify your user name for {machine}"
                ).ask()

                # Save state
                self.save()

            if not machine in self.rc["machines"]:
                self.rc["machines"].append(machine)
                self.save(file="rc")

        return machines

    def configure_ssh_keys(self, machines=[]):
        """
        Helps the user to configure the ssh keys for the different machines. It uses
        subprocess + ssh-keygen because with paramiko Miguel was not able to configure
        a password input for the ssh-keys and he did not want to be tampering with
        passwords and ``getpass``. In this way, all password operations are fully
        handled by ``ssh-keygen``.

        The path to new ssh-keys follow this convention:

        ``~/.ssh/id_<ssh_key_type>_ptool_<machine>``

        Input
        -----
        machines : list
            A list of machines for the ssh-keys to be configured. If none is defined
            use ``self.rc["machines"]``.
        """
        # Load default if necessary
        if not machines:
            machines = self.rc["machines"]

        # Loop through the different machines
        for machine in machines:
            # Set the ssh-keygen command to be run with subprocess
            ssh_dir = os.path.abspath(os.path.expanduser("~/.ssh"))
            ssh = self[machine].get("ssh", {})
            ssh_key_type = ssh.get("key_type", "ed25519")
            ssh_key_path = f"{ssh_dir}/id_{ssh_key_type}_ptool_{machine}"
            command = f"ssh-keygen -t {ssh_key_type} -f {ssh_key_path}"

            # Print status and ssh-keygen command
            questionary.print(
                f"\n\nConfiguring ssh-key for {machine}", style="bold fg:darkgreen"
            )
            print(f"Command: {command}\n")

            # Create ~/.ssh folder if it does not exists, with restrictive permissions
            if not os.path.isdir(ssh_dir):
                os.mkdir(ssh_dir)
                os.chmod(ssh_dir, 0o700)

            # Ask the user if they'd like to reuse an already existing ssh-key
            ssh_key_config_action = questionary.select(
                "For {machine}, select one option",
                choices=[
                    "Reuse an existing ssh-key",
                    "Create a new ssh-key",
                ],
            ).ask()
            print(ssh_key_config_action)
            if ssh_key_config_action == "Reuse an existing ssh-key":
                possible_keys = [
                    os.path.join(r, _f)
                    for r,d,f, in os.walk(ssh_dir)
                    for _f in fnmatch.filter(f, "id_*[!\\.pub]")
                ]

                # Ask user which ssh-key to use
                ssh_key_path = questionary.select(
                    f"Select an ssh-key for {machine}",
                    choices=possible_keys,
                ).ask()

                print(ssh_key_path)

                # Update the rc file to let ptool know which ssh-key to use for which
                # machine
                self.rc["ssh_keys"][machine] = ssh_key_path
                self.save(file="rc")

            else:
                # Checks whether the ssh-key already exits
                if os.path.isfile(ssh_key_path):
                    rewrite_ssh_key = questionary.confirm(
                        f"{ssh_key_path} already exists. Do you want to overwrite it?"
                    ).ask()
                    if rewrite_ssh_key:
                        os.remove(ssh_key_path)
                    else:
                        print(f"Skipping {ssh_key_path} configuration")
                        continue

                # Run the ssh-keygen command
                command = command.split()
                subprocess.run(command, check=True)

                # Update the rc file to let ptool know which ssh-key to use for which
                # machine
                self.rc["ssh_keys"][machine] = ssh_key_path
                self.save(file="rc")

                # Show public key to the user
                questionary.print(f"\nThis is your public key:", style="bold")
                with open(f"{ssh_key_path}.pub") as f:
                    questionary.print(f.read(), style="fg:blue")

                # Help the user with how to register their pub key in the remote machine
                registration_url = ssh.get("registration_url")
                # If remote machine has a registration_url that can be used to submit
                # the pub key, or the documentation on how to do it, print the
                # registration-url
                if registration_url:
                    print(
                        f"Create a new key with the pub key above (copy/paste) in: "
                        f"{registration_url}"
                    )
                # Otherwise, ask the user if they'd like to try transferring their pub
                # key with ssh-copy-id
                else:
                    if questionary.confirm(
                        f"Should I try to transfer the key with ssh-copy-id to "
                        f"{machine}?"
                    ).ask():
                        ssh_copy_id_command = (
                            f"ssh-copy-id -i {ssh_key_path}.pub "
                            f"{self[machine]['user']}@{self[machine]['host']}"
                        ).split()
                        try:
                            subprocess.run(ssh_copy_id_command, check=True)
                        except:
                            print(
                                f"The following command did not work:\n"
                                f"{ssh_copy_id_command}"
                            )
                            print(
                                f"You'll need to transfer the pub key to {machine} "
                                f"somehow, please consult {machine}'s documentation"
                            )
                    else:
                        print(
                            f"You'll need to transfer the pub key to {machine} "
                            f"somehow, please consult {machine}'s documentation"
                        )

            questionary.press_any_key_to_continue().ask()

    def test_ssh_connection(self, machines=[]):
        """
        Tests whether the ssh-keys generated with ``Config`` work. Subprocess +
        ssh-keygen is used instead of paramiko for the same reasons as in the method
        ``self.configure_ssh_keys``.

        Input
        -----
        machines : list
            A list of machines for the ssh-keys to be tested. If none is defined
            use ``self.rc["machines"]``.
        """
        self.init_from_user_config()

        if not machines:
            machines = self.rc["machines"]

        questionary.print("\nTesting ssh keys", style="bold fg:darkgreen")

        test_results = {}

        # Loop through machines
        for machine in machines:
            private_key_path = self.rc["ssh_keys"][machine]
            host = self[machine]["host"]
            user = self[machine]["user"]
            ssh_test_command = f"ssh -i {private_key_path} {user}@{host} -o IdentitiesOnly=yes 'echo {machine} SSH key authentication successful'"

            # Run the ssh test
            result = subprocess.run(ssh_test_command, shell=True)

            # Store result of the test
            test_results[machine] = not (bool(result.returncode))

        # Report results of the tests
        questionary.print("\nssh key test results", style="bold fg:darkgreen")
        for machine, value in test_results.items():
            questionary.print(machine, end=" ")
            if value:
                questionary.print("working", style="green")
            else:
                questionary.print("broken", style="red")

    def all(self):
        """
        Recipe for the full config workflow
        """
        self.create_rc()
        machines = self.create_user_config()
        self.configure_ssh_keys(machines)
        self.test_ssh_connection(machines)
        questionary.print("\nConfiguration finished!", style="bold fg:blue")
