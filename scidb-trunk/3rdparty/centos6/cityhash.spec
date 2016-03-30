%define scidb_cityhash scidb-SCIDB_VERSION_MAJOR.SCIDB_VERSION_MINOR-cityhash
%define scidb_path   /opt/scidb/SCIDB_VERSION_MAJOR.SCIDB_VERSION_MINOR/3rdparty/cityhash
%define short_name   cityhash

Summary:	Fast hash functions for strings
Name:		%{scidb_cityhash}
Version:	1.1.1
Release:	1
License:	MIT
Group:		Libraries
URL:		http://code.google.com/p/cityhash
Source0:	http://downloads.paradigm4.com/centos6.3/3rdparty_sources/%{short_name}-%{version}.tar.gz
BuildRequires:	libstdc++-devel

%define _unpackaged_files_terminate_build 0

%description
CityHash provides hash functions for strings. The functions mix the
input bits thoroughly but are not suitable for cryptography.

%prep
%setup -q -n %{short_name}-%{version}

%global _prefix      %{scidb_path}
%global _datarootdir %{scidb_path}/share
%global _datadir     %{scidb_path}/share
%global _mandir	     %{_datadir}/man
%global _bindir	     %{_prefix}/bin
%global _libdir	     %{_prefix}/lib
%global _includedir  %{_prefix}/include/%{short_name}

%build
%configure \
	--prefix=%{_prefix}					\
	--sysconfdir=%{_sysconfdir}/%{short_name}		\
	--includedir=%{_includedir}				\
	--bindir=%{_bindir}					\
	--libdir=%{_libdir}					\
	--datarootdir=%{_datarootdir}				\
	--datadir=%{_datadir}					\
	--mandir=%{_mandir}					\
	--docdir=%{_datadir}/doc				\
	--htmldir=%{_datadir}/doc				\
	CFLAGS="-g -O3"						\
	CXXFLAGS="-g -O3"
%{__make}

%install
rm -rf %{buildroot}
%{__make} DESTDIR=%{buildroot} install

%clean
rm -rf %{buildroot}

%post	-p /sbin/ldconfig
%postun	-p /sbin/ldconfig

%files
%defattr(644,root,root,755)
%doc NEWS README
%{_includedir}/city.h
%{_libdir}/libcityhash.so
%{_libdir}/libcityhash.so.0
%{_libdir}/libcityhash.la
%{_libdir}/libcityhash.a
%attr(755,root,root) %{_libdir}/libcityhash.so.*.*.*
