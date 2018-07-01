# Build instructions for dbitool.
#
# We need to use ExtUtils::MakeMaker since this module is part of Perl core,
# which only supports that build method, and because it is a dependency of
# other build systems like Module::Build.

#  Copyright 2018 Rodrigo Otavio Rodrigues Antunes
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

use 5.006;
use strict;
use warnings;

use Config;
use ExtUtils::MakeMaker;
use File::Spec;

use Data::Dumper;

# Get the current version from the source code
sub dist_version {
  my($v) = `cat scripts/dbitool.pl` =~ /^\$VERSION\s*=\s*"([\d\.]+)"\s*;\s*$/m;
  return($v);
}

# Generate full paths for scripts distributed in the bin directory.
# Returns: (Array) List of relative paths from top of distribution
#          (Scalar) Space-separated relative paths from top of distribution
sub scripts {
  my (@scripts) = @_;
  my @paths = map { File::Spec->catfile('scripts', $_) } @scripts;
  return wantarray ? @paths : join(q{ }, @paths);
}

# Generate an association between a source file and a destination man page for
# non-module man pages.  ExtUtils::MakeMaker only really understands how to
# generate man pages for modules, so we need to help it for the script man
# pages and (particularly) the perlpodstyle man page.
#
# $directory - Directory containing the file
# $file      - File containing POD in that directory
#
# Returns: The path to the file with POD and the output man page, as a pair
sub man1pod {
  my ($directory, $file) = @_;

  # Build the base name of the file by stripping any *.pod or *.PL suffix.
  my $basename = $file;
  $basename =~ s{ [.] (?: pod | PL ) \z }{}xms;

  # Determine the output file name for the generated man page.
  my $outname = $basename . q{.} . $Config{man1ext};
  my $outpath = File::Spec->catfile(qw(blib man1), $outname);
  return (File::Spec->catfile($directory, $file), $outpath);
}

# The hash of all the metadata.  This will be modified before WriteMakefile to
# remove keys not supported by the local version of ExtUtils::MakeMaker.
my $dist_version = dist_version();
my %metadata     = (
  NAME             => 'dbitool',
  DISTNAME         => 'dbitool',
  ABSTRACT         => 'Manage data sources and destinations using DBI classes',
  AUTHOR           => 'Rodrigo O R Antunes <x@rora.com.br>',
  LICENSE          => 'Apache 2.0',
  EXE_FILES        => [scripts('dbitool')],
  VERSION_FROM     => 'scripts/dbitool.pl',
  MIN_PERL_VERSION => '5.006',

  # Use *.pl files to generate the driver scripts so that we get the correct
  # invocation of Perl on non-UNIX platforms.
  PL_FILES => {
    scripts('dbitool.pl', 'dbitool')
  },

  # Override the files that generate section 1 man pages.
  MAN1PODS => {
    man1pod('scripts', 'dbitool.pl'),
  },

  # Clean some additional files.
  clean     => { FILES => File::Spec->catdir('t',    'tmp') },
  realclean => { FILES => scalar(scripts('dbitool')) },

  # Dependencies on other modules.
  PREREQ_PM => {
    'Cwd' => 3.63,
    'DB_File' => 1.835,
    'DBI' => 1.636,
    'Getopt::Long' => 2.48,
    'IO::File' => 1.16,
    'IO::Handle' => 1.36,
    'IO::Select' => 1.22,
    'Pod::Usage' => 1.68
  },

  # Test directory
  test => { TESTS => 't/*.t' },

  # For older versions of Perl, we have to force installation into the Perl
  # module directories since site modules did not take precedence over core
  # modules.
  INSTALLDIRS => $] lt '5.011' ? 'perl' : 'site',

  # Additional metadata.
  META_ADD => {
    'meta-spec' => { version => 2 },
    provides    => {
      'dbitool' => {
        file    => 'dbitool',
        version => $dist_version,
      },
    },
    resources => {
      homepage   => 'https://github.com/rorabr/dbitool',
      repository => {
        url  => 'https://github.com/rorabr/dbitool.git',
        web  => 'https://github.com/rorabr/dbitool',
        type => 'git',
      },
    },
  },
);

# Generate the actual Makefile.
WriteMakefile(%metadata);
