#!/bin/bash

set -e -x

WTSI_NPG_GITHUB_URL=$1
WTSI_NPG_BUILD_BRANCH=$2

# deal with circular npg repo dependencies
REPODIR=$PWD
PERL5LIB=$PWD/blib/lib:$PWD/lib

eval $(perl -I ~/perl5ext/lib/perl5/ -Mlocal::lib=~/perl5ext)
cpanm --quiet --notest Alien::Tidyp # for npg_qc
cpanm --quiet --notest Module::Build
cpanm --quiet --notest Net::SSLeay
cpanm --quiet --notest LWP::Protocol::https
cpanm --quiet --notest https://github.com/chapmanb/vcftools-cpan/archive/v0.953.tar.gz

# WTSI NPG Perl repo dependencies
# Dependency on perl-irods-wrap comes from npg_qc
repo_names="perl-dnap-utilities ml_warehouse npg_tracking npg_seq_common perl-irods-wrap npg_qc"

for repo in $repo_names
do
  # Logic of keeping branch consistent was taken from @dkj
  # contribution to https://github.com/wtsi-npg/npg_irods
  cd /tmp
  # Always clone master when using depth 1 to get current tag
  git clone --branch master --depth 1 ${WTSI_NPG_GITHUB_URL}/${repo}.git ${repo}.git
  cd /tmp/${repo}.git
  # Shift off master to appropriate branch (if possible)
  git ls-remote --heads --exit-code origin ${WTSI_NPG_BUILD_BRANCH} && git pull origin ${WTSI_NPG_BUILD_BRANCH} && echo "Switched to branch ${WTSI_NPG_BUILD_BRANCH}"
  repos=$repos" /tmp/${repo}.git"
done

for repo in $repos
do
    export PERL5LIB=$repo/blib/lib:$PERL5LIB:$repo/lib
done

cd $REPODIR
perl Build.PL && ./Build # some dependencies are on versions, so need in blib

for repo in $repos
do
    cd $repo
    cpanm  --quiet --notest --installdeps .
    perl -I. Build.PL # -I. for npg_irods
    ./Build
done

# Finally, bring any common dependencies up to the latest version and
# install

# to set liblocal for perl5_npg
eval $(perl -I ~/perl5ext/lib/perl5/ -Mlocal::lib=~/perl5npg)
for repo in $repos
do
    cd $repo
    cpanm  --quiet --notest --installdeps .
    ./Build install
done
cd
