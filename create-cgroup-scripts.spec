Name:		create-cgroup-scripts
Version:	1.0
Release:	1%{?dist}
Summary:	EayunStack Create Cgroup Scripts

Group:		Application
License:	GPL
URL:		http://www.eayun.com
Source0:	create-cgroup-scripts-1.0.tgz

BuildRequires:	/bin/bash
Requires:	python
Requires:	numactl

%description
EayunStack Create Cgroup Scripts


%prep
%setup -q


%build


%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/etc/create-cgroup-scripts/
mkdir -p %{buildroot}/opt/create-cgroup-scripts/
install -p -D  isolate_resource.conf %{buildroot}/etc/create-cgroup-scripts/
install -p -D -m 755 isolate_resource.py %{buildroot}/opt/create-cgroup-scripts/


%files
%doc
/etc/create-cgroup-scripts/isolate_resource.conf
/opt/create-cgroup-scripts/
%attr(0755,root,root)/opt/create-cgroup-scripts/isolate_resource.py


%changelog
* Thu Nov 2 2017 Ma Zhe	<zhe.ma@eayun.com> 1.0-1
- init version
