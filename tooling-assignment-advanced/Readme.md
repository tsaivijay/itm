# Tooling Assignment Advanced

## Objectives

* Demonstrate the use of Virtual Machines and discuss how they can extend your PCs capabilities
* Discuss the tools used for x86 based Virtualization and Apple Silicon based Virtualization
* Examine and explain the benefits of adding an abstraction layer on top of Virtualization platforms
* Explain the uses and advantages of the Vagrant platform in relation to virtual machine management

## Outcomes

At the conclusion of this lab you will have investigated using a virtualization platform (x86 VirtualBox and M1 Parallels) and demonstrated the ability to extend your PCs capabilities using a Virtual Machine.  You will have implemented a single abstraction layer on top of your virtualization platform.  You will have discussed the advantages of using Vagrant and you will have implemented the tool and deployed virtual machine images.

### Vagrant

[Taken from Intro to Vagrant](https://www.vagrantup.com/intro "Intro to Vagrant web page")

> Vagrant is a tool for building and managing virtual machine environments in a single workflow. With an easy-to-use workflow and focus on automation, Vagrant lowers development environment setup time, increases production parity, and makes the "works on my machine" excuse a relic of the past.

#### Why Vagrant

> Vagrant provides easy to configure, reproducible, and portable work environments built on top of industry-standard technology and controlled by a single consistent workflow to help maximize the productivity and flexibility of you and your team.

#### For Developers

If you are a developer, Vagrant will isolate dependencies and their configuration within a single disposable, consistent environment, without sacrificing any of the tools you are used to working with (editors, browsers, debuggers, etc.). Once you or someone else creates a single Vagrantfile, you just need to vagrant up and everything is installed and configured for you to work. Other members of your team create their development environments from the same configuration, so whether you are working on Linux, Mac OS X, or Windows, all your team members are running code in the same environment, against the same dependencies, all configured the same way. Say goodbye to *"works on my machine"* bugs.

#### For Operators

If you are an operations engineer or DevOps engineer, Vagrant gives you a disposable environment and consistent workflow for developing and testing infrastructure management scripts. You can quickly test things like shell scripts, Chef cookbooks, Puppet modules, and more using local virtualization such as VirtualBox or VMware. Then, with the same configuration, you can test these scripts on remote clouds such as AWS or RackSpace with the same workflow. Ditch your custom scripts to recycle EC2 instances, stop juggling SSH prompts to various machines, and start using Vagrant to bring sanity to your life.

#### For Designers

If you are a designer, Vagrant will automatically set everything up that is required for that web app in order for you to focus on doing what you do best: design. Once a developer configures Vagrant, you do not need to worry about how to get that app running ever again. No more bothering other developers to help you fix your environment so you can test designs. Just check out the code, vagrant up, and start designing.

#### Vagrant Summary

Vagrant is an abstraction layer -- that sits on top of any virtualization platform and streamlines the workflow

### Vagrant and VirtualBox History

VirtualBox was a tool first released in 2007 that enabled you to Virtualize or share your system hardware with a second operating system. This is done via a software layer called a Hypervisor that intercepts OS calls to hardware allowing your system to have multiple OSes installed.  Essentially the Hypervisor is lying to each installed OS -- telling each OS that it is the only OS installed while hiding this fact from each other and sharing the hardware, memory, CPU, and network amongst them. This is possible since the majority of PCs are very powerful and are actually idle most of the time, this allows for the sharing of resources to take place.

While VirtualBox presents many options and capabilities, if all we want to do is work on our application and not worry about the operating system then we need to find a way to abstract away all of those options.  Vagrant, from HashiCorp, is that tool.

Vagrant was created in ~2010 by two college students in their dorm having to do what you are doing now.  They wanted to code their assignments up, install software to support their assignments, and not have each classes tools interfere with each other. Originally, Vagrant only supported VirtualBox, but over the years Vagrant support has spread to cover essentially all the possible Desktop Virtualization platforms ([Type II](https://en.wikipedia.org/wiki/Hypervisor#Classification "Type II wiki article")).

Vagrant can be used to manage pre-made virtual machine artifacts or custom made virtual machine artifacts made from the Packer tool. HashiCorp provides pre-made virtual machine artifacts via the [https://app.vagrantup.com/boxes/search](https://app.vagrantup.com/boxes/search "Vagrant website") website.  Here you can search for primarily ready made Linux and some FreeBSD images provided by Distro makers and community members.  Note that most instances are provided for x86 Macs and Windows, but you can search for M1 based vms for Parallels.

### Vagrant Basics

Remember that Vagrant is an abstraction tool -- it doesn't control or create anything itself, only interacts with a Virtualization tool (VirtualBox or Parallels).  

There are a few basic terms to remember when dealing with Vagrant:

* Box file
  * a \*.box file contains a compressed virtual hard disk and a compressed configuration file inside of a single file.  This is how Vagrant distributes the artifacts it manages
* Vagrantfile
  * This is a file (note the capital 'V') that contains a virtual machines hardware configuration

### Vagrant Commands

Assuming that the command `vagrant --version` gives us output, lets begin by installing our first Vagrant Box.  Open your terminal application and let us `cd` to the Documents directory

```cd Documents```

Here we are going to create a directory to manage our artifact.  It is a good idea to create a directory per virtual machine that we will administer via Vagrant.  You can create a class directory and then sub-directories and or you can place this on a different disk. This I will leave up to you as it is your filesystem and your data--you are the one in charge.

```mkdir itmd-521 ; cd itmd-521```

We will now use Vagrant to retrieve 2 Linux Distributions and Ubuntu 22.04 known as Jammy and a [Alma Linux 9](https://wiki.almalinux.org/release-notes/9.1.html "webpagte release notes for Alma Linux 9") (CentOS/Red Hat based) Virtual Machines:

* ```mkdir jammy64 ; cd jammy64 ; vagrant init ubuntu/jammy64 ; ls```
* ```mkdir almalinux9 ; cd almalinux9 ; vagrant init almalinux/9 ; ls```

For those using M1 Macs and Parallels you will need to replace the names of the Boxes in the demos with these two that have been prepared for M1 macs and parallels

* bento/debian-11.2-arm64
* almalinux/9.aarch64

### Additional Parallels M1 requirements

Parallels is a Apple Silicon native virtualization solution, equal in all senses to VirtualBox on x86 hardware. It does require a couple of extra items detailed at this [KB 122843 article](https://kb.parallels.com/en/122843 "webpage for parallels kb article").

### Edit the Vagrantfile

Once these commands are executed -- both under the `itmd-521` directory, you will see a Vagrantfile that has been created.  Let us take a look at this file. You can do so via using the commands on MacOS or Windows from the Terminal:

* `code Vagrantfile`
* `atom Vagrantfile`
* `vim Vagrantfile`
  * You can use chocolatey to install `vim` on Windows -- it works well I use it all the time

Line 15 you will see the setting that tells Vagrant which **box** this Vagrantfile manages: `config.vm.box = "ubuntu/jammy64"`. This value came from the `vagrant init` command typed above. Line 35, which is commented out, will let us configure a private local network between out host system and any guest (virtual) OSes we install.  Line 52, 57, and 58 are a loop that allows us to increase the default memory from 1Gb to 2 Gb or 4 Gb. For now lets not make any changes.

### Start a Vagrant Box

From our jammy64 directory, let us start our first Vagrant Box.  From the Terminal type: `vagrant up`.  What you will see is the Box file with the VirtualBox (or Parallels) vm being extracted and registered with your virtualization software.  Next the system will begin to boot.  The first install will take a bit longer as some additional drivers are being installed.  This only happens on first boot.

Once this step is successful, we need to establish a connection to the virtual machine via SSH (secure shell). We do this by the command: `vagrant ssh`, and we are faced with an Ubuntu Server command prompt. What was the password? What was the IP address?  You don't know and don't need to know as Vagrant has abstracted all of this away and allowed you to get to the focus of all of this -- installing and running software. Open a new Terminal window and repeat the steps above for the Alma Linux 9 box.

### Additional Vagrant Commands

* To exit this ssh session type: `exit`
* From the host OS, to restart a Vagrant Box you would type: `vagrant reload`
* From the host OS, to pause or place into standby you would type: `vagrant susupend`
* From the host OS, to bring out of standby your would type: `vagrant resume or vagrant up`
* From the host OS, to power-off your virtual machine you would type: `vagrant halt`
* From the host OS, to remove all changes and reset the box to the status at first install you would type: `vagrant destroy`
* From the host OS, to remove the Vagrant Box entirely from Vagrant's control you would type: `vagrant box remove <nameofbox>`
* From the host OS, to list all of the boxes managed by Vagrant you would type: `vagrant box list`

### Modify Settings in the Vagrantfile

The Vagrantfile is only processed the first time a system is initialized via `vagrant up`.  If you make a change you need to reboot or power down/up the machine and for the first time after the change you would add the flag `--provision` to the command to force Vagrant to reinitialize the hardware.

Let us try this.  Choose the Ubuntu 22.04 jammy64 system's Vagrantfile and let us open it for editing.  Let us uncomment line 35 and let us uncommand line 52, 57, and 58, changing the value on line 57 to 4096 if you have the extra memory or 2048 at least.  If your jammy virtual machine is running, form the host OS issue the command: `vagrant reload --provision` or if powered off `vagrant up --provision`.

To further check the results after the command `vagrant ssh` is issued from the Ubuntu CLI type the command: `free --giga` to see how much memory is in the system.  To test the private network, let us install a webserver by issuing the command: `sudo apt update; sudo apt install nginx`.  From your host OS, open a web-browser to `http://192.168.56.10` and you will be met by a Welcome to Nginx message.

If you are using M1 Apple Silicon - the private network settings are bit different.  Here is a link to the Paralles documentation for [setting a Private Network interface static IP](https://parallels.github.io/vagrant-parallels/docs/networking/private_network.html "webpage for Parallels Vagrant Private Networking").

```
Vagrant.configure("2") do |config|
  config.vm.network "private_network", ip: "192.168.50.4"
end
```

### Reset a Virtual Machine

If you want to reset your Vagrant Box after you installed a webserver.  Exit the ssh session and from the command line of the Host OS issue the commmand: `vagrant destroy ; vagrant up ; vagrant ssh` and you will find once that process is complete that the webserver software that was installed is now gone: `sudo systemctl status nginx.service` will report no service found.

This is handy because often you want a fresh server to install some tools, but don't want to take the 25 minutes to reinstall all the Operating System.  The command `vagrant destroy` will in a matter of moment, discard all the changes since the initial `vagrant up` and reset the vm to that point.  Very handy for experimentation and a quick reset.

## From your host system

* From the command line (non-admin) execute the command: `vagrant plugin install vagrant-vbguest`
  * This takes care a warning message from Vagrant about not being able to mount VirtualBox shared drives
* Configure Vagrant Box memory to use at least 2 GB

## Inside Vagrant Box Steps

You will need to use the `wget` commandline tool to retrieve installation files for Hadoop and Spark.  You will need to use the `tar` command to extract the tarballs: `tar -xvzf` and the command `sudo apt update` to check for the latest packages and the command: `sudo apt install` to install additional packages like Java and MariaDB. For Alma Linux you will need to install additional tools: ```sudo dnf install epel-release vim rsync wget```

* Change hostname of the system to include the course number and your initials
  * Use the command: `sudo hostnamectl set-hostname initials-and-class-number`
* Add these values to your `.bashrc` file located in your Home directory: `/home/vagrant/.bashrc`
  * `export JAVA_HOME=/usr`
  * `export SPARK_HOME=/home/vagrant/spark`
  * `export PYSPARK_PYTHON=python3`
  * `export PATH=$PATH:$SPARK_HOME/bin`
* Install MariaDB server
  * `mariadb-server`
* Install Java 11 OpenJDK
  * `sudo apt update`
  * `sudo apt install openjdk-11-jdk`
* Install R package
  * Ubuntu - use `apt` to install `r-base`
  * AlmaLinux use: `sudo dnf config-manager --set-enabled crb; sudo dnf install R`
* Install Spark 3.2.x
  * [https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.2.tgz](https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz "Apache Spark Download Link")
  * You will need to install the `git` executable as well

## Summary

Today we learned how to use Vagrant for managing virtual machine artifacts. We learned how to extend our PCs capabilities by enabling us to install additional software.
