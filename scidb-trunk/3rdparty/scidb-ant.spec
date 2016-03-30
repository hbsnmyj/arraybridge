Name:      scidb-VERSION-ant
Summary:   Apache Ant used for building JDBC
Version:   0
Release:   1
License:   GPLv3
Group:     System Environment/Base
URL:       https://downloads.paradigm4.com
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildArch: noarch

%description
Apache Ant used for building JDBC

%prep

%build

%install
rm -rf %{buildroot}
install -dm 755 %{buildroot}/opt/scidb/VERSION/3rdparty
tar -xpf %{_sourcedir}/*.bz2 -C %{buildroot}/opt/scidb/VERSION/3rdparty

%clean

%files
%defattr(-,root,root,-)
%attr(-,root,root) /opt/scidb/VERSION/3rdparty/*

%changelog
* Thu Apr 30 2015 SciDB support list <support@lists.scidb.org>
- Initial build.
