#!/usr/bin/perl -I/home/rora/projetos/perllib

# dbitool - Manage data sources and destinations using DBI classes - RORA - Montreal - 2018-05-16

$VERSION = "0.1.0";

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

$main::debugTrace = 0;

use 5.006;
use warnings;
use Getopt::Long;
use Pod::Usage;
use Carp;
use JSON;
use IO::Handle;
use IO::File;
use DBI;
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

# defaults
my %args_defaults=(
      streamsize => 1024,              # number of rows read/write in each step
      errorsize => 3,                  # limit number of errors before abort
      memorylimit => 100000,           # limit number of rows in memory buffers
      loglevel => 1,                   # default log level if -v if not used
      listmodules => 0,                # flag for --listmodules
      help => 0,                       # flag for --help
      verbose => 0,                    # number of -v
    );
# parse command line
my @args_syntax=(qw/listmodules|l streamsize|s:i errorsize|e:i memorylimit|m:i loglevel|l:i help|h verbose|v+/);
my $parser = Getopt::Long::Parser->new;
if (! $parser->getoptions(\%args_defaults, @args_syntax) || $args_defaults{help}) {
  pod2usage(-exitval=>1, -verbose=>1);
}
$main::time = dbitooltime->new; # time ctrl singleton
$main::streamlist = streamlist->new(max => $args_defaults{streamsize}); # stream factory singleton
my $self=bless({%args_defaults}, __PACKAGE__);
die("$0 sintax error: invalid streamsize (1 .. 65536)") if ($self->{streamsize} < 1 || $self->{streamsize} > 65536);
die("$0 sintax error: invalid errorsize (1 .. 256)") if ($self->{errorsize} < 1 || $self->{errorsize} > 256);
die("$0 sintax error: invalid memorylimit (1 .. 1000000)") if ($self->{memorylimit} < 1 || $self->{memorylimit} > 1000000);
die("$0 sintax error: invalid loglevel (0 .. 3)") if ($self->{loglevel} < 0 || $self->{loglevel} > 3);
$self->listmodules if ($self->{listmodules});
$self->parse;
$self->run;
$self->close;
die("$0: $self->{errorcount} error(s)") if ($self->{errorcount});

# Parse the command line and create modules and streams
sub parse {
  my($self)=@_;
  $main::nextStream = 1;
  $main::streamLastOut = "";
  my $loglevel = $self->{loglevel} > $self->{verbose} ? $self->{loglevel} : $self->{verbose};
  my $createlog = 0;
  my $hasstdout = 0;
  eval {
    my $tree;
    my $n = 1;
    while (@ARGV) {
      my $a = shift(@ARGV);
      if ($a =~ /(\w+)(:(.*))?$/) {
        my $name = $1;
        my $args = $3 || "";
        my $pkg = "dbitoolmod_" . $name;
        eval {
          my $mod = $pkg->new(max=>$self->{streamsize}, args=>$args, name=>"$name$n", memorylimit=>$self->{memorylimit}, verbose=>$loglevel);
          $mod->setup;
          $createlog = 1 if ($mod->{in} eq "log");
          $hasstdout = 1 if ($pkg eq "dbitoolmod_stdout");
          push @{$self->{module}}, ($mod);
          $main::streamLastOut = "" if ($mod->{out_type} eq "none");
          push @{$tree->{$mod->{in}}}, ({mod=>$mod, out=>$mod->{out}});
          $n++;
          # check if input stream is a file - shortcut to fileread
          # check if out stream is a file - shortcut to filewrite
          if ($mod->{out} =~ /^\@([^\@]+)$/) {
            die("cannot shortcut filename in fileread or filewrite modules") if ($mod->{name} =~ /^file/);
            my($fname) = ($1, $2);
            my $ofmod = dbitoolmod_filewrite->new(max=>$self->{streamsize}, args=>"in=$mod->{out}:out=$fname:text=1", name=>"filewrite$n\@", verbose=>$loglevel);
            # TODO: text=1 should be read from somewhere and not hardcoded...
            $ofmod->setup; # no ref to handmaid's!
            push @{$self->{module}}, ($ofmod);
            push @{$tree->{$ofmod->{in}}}, ({mod=>$ofmod, out=>$ofmod->{out}});
            $main::streamLastOut = "" if ($ofmod->{out_type} eq "none");
            $n++;
          }
        };
        if ($@) {
          if ($@ =~ /^Can't locate object method "new" via package "$pkg/) {
            die("module $name does not exist, use --listmodules to show all available source and destination modules");
          }
          die("$@");
        }
      } else {
        die("syntax error in command line at \"$a\", use --help");
      }
    }
    die("cannot use -v with a stdout module") if ($self->{verbose} && $hasstdout);
    # create the error module and streams if -v
    $main::error = dbitoolmoderror->new(name=>"error", out_streamname => "error", max=>$self->{streamsize}); # create error log singleton
    push @{$self->{module}}, ($main::error);
    push @{$tree->{"_err"}}, ({mod=>$main::error, out=>"error"});
    #if ($self->{verbose}) {
      my $ecsvwrite = dbitoolmod_csvwrite->new(name=>"errcsvwrite", sep=>" ", header=>0, fname=>"", max=>$self->{streamsize}, args=>"in=error:out=err_raw");
      $ecsvwrite->setup;
      push @{$self->{module}}, ($ecsvwrite);
      push @{$tree->{"error"}}, ({mod=>$ecsvwrite, out=>"err_raw"});
      my $stderr = dbitoolmod_stderr->new(name=>"errstderr", text=>1, max=>$self->{streamsize}, args=>"in=err_raw");
      $stderr->setup;
      push @{$self->{module}}, ($stderr);
      push @{$tree->{"err_raw"}}, ({mod=>$stderr, out=>""});
    #}
    $self->setlog($main::error);
    # create the log module and streams if -v
    if ($createlog || $self->{verbose}) {
      $main::log = dbitoolmodlog->new(max=>$self->{streamsize}); # create log singleton
      push @{$self->{module}}, ($main::log);
      push @{$tree->{"_log"}}, ({mod=>$main::log, out=>"log"});
      if ($self->{verbose}) {
        my $csvwrite = dbitoolmod_csvwrite->new(name=>"logcsvwrite", sep=>" ", header=>0, fname=>"", max=>$self->{streamsize}, args=>"in=log:out=log_raw");
        $csvwrite->setup;
        push @{$self->{module}}, ($csvwrite);
        push @{$tree->{"log"}}, ({mod=>$csvwrite, out=>"log_raw"});
        my $stdout = dbitoolmod_stdout->new(name=>"logstdout", text=>1, max=>$self->{streamsize}, args=>"in=log_raw");
        $stdout->setup;
        push @{$self->{module}}, ($stdout);
        push @{$tree->{"log_raw"}}, ({mod=>$stdout, out=>""});
      }
      $self->setlog($main::log);
    }
    if ($createlog || $self->{verbose}) {
      &main::l("main", "module connections:");
      $self->printtree($tree, $_) for ($self->{module}->[0]->{in}, "_log", "_err");
    }
    $main::streamlist->check;
  };
  die("$0 parse error: $@") if ($@);
}

# List all current available modules, formats and exit
sub listmodules {
  my($self)=@_;
  no strict "refs";
  foreach my $k (sort keys %::) {
    if ($k =~ /^dbitoolmod_(\w+)::$/) {
      my $name = $1;
      my $pkg = "dbitoolmod_$name";
      my %args = $pkg->args; 
      printf("module %-16s  %-4s  =>  %-4s  %s\n", $name, $args{in_type}, $args{out_type}, $args{in_memory}? "(in memory)":"");
    }
  }
  printf("default row reads/writes per module iteration: %d\ndefault error limit count: %d\ndefault row limit for in memory modules: %d\n", $self->{streamsize}, $self->{errorsize}, $self->{memorylimit});
  exit(0);
}

# Print the tree of modules if in verbose mode
sub printtree {
  my($self, $tree, $in_name, $indent)=@_;
  $indent ||= 0;
  for my $r (@{$tree->{$in_name}}) {
    &main::l("main", "  " . sprintf("%s%s", ($indent > 0 ? (" " x 6) x ($indent - 1) . "  |-> " : ""), $r->{mod}->tostring));
    #print sprintf("%s%s", ($indent > 0 ? (" " x 6) x ($indent - 1) . "  |-> " : ""), $r->{mod}->tostring) . "\n";
    if ($r->{mod}->{out} ne "") {
      $self->printtree($tree, $r->{mod}->{out}, $indent + 1);
    }
  }
}

# Set log_stream flag on the module log tree
sub setlog {
  my($self, $mod)=@_;
  $mod->{log_stream} = 1;
  if (defined $mod->{out_stream}) {
    $mod->{out_stream}->{log_stream} = 1;
    for my $m (@{$self->{module}}) { # TODO: improve this!
      $self->setlog($m) if (defined $m->{in_stream} && $m->{in_stream}->{name} eq $mod->{out_stream}->{name});
    }
  }
}

# Step thru all output modules until all finished
sub run {
  my($self)=@_;
  eval {
    $self->{errorcount} = 0;
    &main::l("main", "dbitool start");
    $self->{time0} = $main::time->now;
    $_->open for @{$self->{module}};
    print Dumper($self) if ($main::debugTrace); #exit;
    my($run, $closelog);
    do {
      $run = 0;
      $closelog = 1;
      if ($main::debugTrace) {
        for my $k (keys %{$main::streamlist->{list}}) {
          my $stream = $main::streamlist->{list}->{$k};
          printf("DBG> stream:%-17s log:%d eof:%d buf:%d\n", $k, $stream->{log_stream}, $stream->{eof}+0, ($#{$stream->{buf}->[0]})+1);
        }
        for my $mod (@{$self->{module}}) {
          printf("DBG> module:%-17s log:%d eof:%d in:%-12s out:%-12s in_col:%s out_col:%s\n", $mod->{name}, $mod->{log_stream}, $mod->{eof}+0, $mod->{in}, $mod->{out}, join(",", @{$mod->{in_col}}), join(",", @{$mod->{out_col}}));
        }
      }
      for my $mod (@{$self->{module}}) {
        eval {
          if (((defined $mod->{in_stream} && $#{$mod->{in_col}} >= 0) || ! defined $mod->{in_stream}) &&
              ((defined $mod->{out_stream} && $#{$mod->{out_col}} >= 0) || ! defined $mod->{out_stream})) {
            $mod->step if (! $mod->{eof});
            $run = 1 if (! $mod->{eof});
            $closelog = 0 if (! $mod->{eof} && ! $mod->{log_stream});
          } else {
            $mod->prepincolname if ($#{$mod->{in_col}} < 0 && defined $mod->{in_stream});
            $mod->prepoutcolname if ($#{$mod->{out_col}} < 0 && defined $mod->{out_stream});
            $run = 1;
            $closelog = 0;
          }
        };
        if ($@) {
          $main::error->write([$main::time->datetime(), "[error]", "in module $mod->{name}: $@"]);
          $self->{errorcount} ++;
          if ($self->{errorcount} >= $self->{errorsize}) {
            $main::error->write([$main::time->datetime(), "[error]", "maximum number of errors (count:$self->{errorcount}, limit:$self->{errorsize}) reached, aborting dbitool"]);
            $mod->{in_stream} = undef;
            $mod->{eof} = 1;
            $closelog = 1;
          }
        }
      }
      if ($closelog) {
        $main::error->seteof;
        if (defined $main::log && ! $main::log->{eof}) {
          my $e = $main::time->interval($self->{time0});
          &main::l("main", sprintf("dbitool stop in %.3fs", $e));
          $main::log->seteof;
        }
      }
      print "DBG> run:$run closelog:$closelog\n" if ($main::debugTrace);
    } while ($run);
  };
  die("$0 run error: $@") if ($@);
}

# Call close in all modules
sub close {
  my($self)=@_;
  eval {
    $_->close for (@{$self->{module}});
  };
  die("$0 shutdown error: $@") if ($@);
}

# Insert new log message in the log module
sub l {
  return if (! defined $main::log);
  my($mod, $msg)=@_;
  $msg =~ s/([\0-\x1f])/"^" . pack("C", unpack("C",$1)+64)/gse;
  $main::log->write([$main::time->datetime(), "[".$mod."]", $msg]);
}

####### Modules and streams classes

package dbitoolbase;

# Base class (modules and streams included)

use Data::Dumper;
use IO::Select;
use Carp qw/longmess croak/;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    name => "",                        # module name
    #opened => 0,                       # the stream/module is opened
    rows => 0,                         # number of rows (if available > 0)
    in_col => [],                      # input column names
    out_col => [],                     # output column names
    log_stream => 0,                   # if 1 is in the log stream
  );
}

sub new {
  my($class)=shift;
  my $self = bless({&dbitoolbase::args, @_}, $class);
}

# Require the necessary module or bail out
sub requirepackage {
  my($self, $m)=@_;
  eval {
    my $f = $m;
    $f =~ s/::/\//gs;
    require($f . ".pm");
  };
  if ($@) {
    if ($@ =~ /^Can't locate $m /) {
      $self->die("cannot load because of unmet dependencies: \"$m\"");
    }
    die($@);
  }
}

## Return an array with column names
#sub getcol {
#  my($self, $dir)=@_;
#  #return($self->{col}) if ($#{$self->{col}} >= 0);
#  return($self->{col});
#}

# Set the column names
sub setcol {
  my($self, $dir, $col)=@_;
  $self->{$dir . "_col"} = $col;
}

# Print stack or msg and die
sub die {
  my($self, $msg)=@_;
  if ($self->{verbose}) {
    die longmess("in module $self->{name}, $msg");
  } else {
    croak("in module $self->{name}, $msg");
  }
}

#sub tostring {
#  my($self)=@_;
#  sprintf("%s", $self->{name});
#}

1;

package dbitooltime;

# no Time::HiRes available encapsulation calls

use base 'dbitoolbase';
use strict;

sub new {
  my($class)=shift;
  eval { # TODO: not needed?
    require("Time/HiRes.pm")
  };
  if (eval("defined Time::HiRes::import")) {
    return(dbitooltimehires->new(@_));
  }
  my $self = bless(dbitoolbase->new(@_), $class);
}

sub now {
  return([time]);
}

sub interval {
  my($self, $time0)=@_;
  return(time - $time0->[0]);
}

sub datetime {
  my($self)=@_;
  my @t = localtime(time);
  return(sprintf("%04d-%02d-%02d_%02d:%02d:%02d", $t[5]+1900, $t[4]+1, $t[3], $t[2], $t[1], $t[0]));
}

1;

package dbitooltimehires;

# Time::HiRes encapsulation calls

use base 'dbitoolbase';
use strict;

sub new {
  my($class)=shift;
  my $self = bless(dbitoolbase->new(@_), $class);
}

sub now {
  return([Time::HiRes::gettimeofday()]);
}

sub interval {
  my($self, $time0)=@_;
  return(Time::HiRes::tv_interval($time0, [Time::HiRes::gettimeofday()]));
}

sub datetime {
  my($self)=@_;
  my $m = Time::HiRes::time();
  my @t = localtime(int($m));
  return(sprintf("%04d-%02d-%02d_%02d:%02d:%02d.%03d", $t[5]+1900, $t[4]+1, $t[3], $t[2], $t[1], $t[0], ($m -int($m)) * 1000));
}

1;

package dbitoolstreamobj;

# Base class for streaming objects - manages data access and buffers

use base 'dbitoolbase';
use if $main::debugTrace, "debugClass", qw/trace/;
use Data::Dumper;
use strict;

sub args {
  return(
    next => 0,                         # index of next registered in_stream for multiple readers
    eof => 0,                          # true if EOF has been reached in all buffers
    buf => [],                         # array of buffers
    max => 0,                          # max size of each buffer (from main)
  );
}

sub new {
  my($class)=shift;
  my $self = bless(dbitoolbase->new(&dbitoolstreamobj::args, @_), $class);
  die("invalid stream buffer size ($self->{max})") if ($self->{max} <= 0);
  return($self);
}

# Return an array with column names
sub getoutcolname {
  my($self)=@_;
  #return($self->{col}) if ($#{$self->{col}} >= 0);
  return($self->{out_col});
}

# Returns true if there's still space in the buffer for $objno
sub canput {
  my($self, $objno)=@_;
  return($#{$self->{buf}->[($objno || 0)]} < $self->{max});
}

# Store new data in all the buffers
sub put {
  my($self, $data)=@_;
  for (my $i = 0; $i < $self->{next}; $i++) {
    push @{$self->{buf}->[$i]}, ($data);
  }
}

# Return true if there is data for objno
sub canget {
  my($self, $objno)=@_;
  return($#{$self->{buf}->[$objno]} >= 0);
}

# Return oldest data from one of the buffers or undef
sub get {
  my($self, $objno)=@_;
  return(shift(@{$self->{buf}->[$objno]}));
}

# Return true if this instance has reached EOF for objno
sub eof {
  my($self, $objno)=@_;
  return($self->{eof} && $#{$self->{buf}->[$objno]} < 0);
}

# Instantiate a new stream reader class linked to this one
sub register {
  my($self)=@_;
  return(streamreader->new(stream=>$self, name=>$self->{name}, no=>$self->{next}++, in_type=>$self->{in_type}, out_type=>$self->{out_type}));
}

## Set the log_stream flag on all out stream modules
#sub setlog {
#  my($self)=@_;
#  $_->{setlog} = 1 for (@{$self->{out_mod}});
#}

1;

package streamreader;

# Class instantiaded by register to interface with streamobj on multiple reads

use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub new {
  my($class)=shift;
  my $self = bless({@_}, $class);
}

#sub getcol {
#  my($self)=@_;
#  return($self->{stream}->getcol);
#}

# Return an array with column names
sub getoutcolname {
  my($self)=@_;
  return($self->{stream}->getoutcolname);
}

sub setcol {
  my($self, $dir, $col)=@_;
  $self->{stream}->setcol($dir, $col);
}

sub eof {
  my($self)=@_;
  return($self->{stream}->eof($self->{no}));
}

sub canget {
  my($self)=@_;
  return($self->{stream}->canget($self->{no}));
}

sub get {
  my($self)=@_;
  return($self->{stream}->get($self->{no}));
}

sub canput {
  my($self)=@_;
  return($self->{stream}->canput($self->{no}));
}

sub put {
  my($self, $data)=@_;
  return($self->{stream}->put($data));
}

1;

package streamlist;

# Singleton/factory class to manage all the streams

use base 'dbitoolbase';
use strict;

sub new {
  my($class)=shift;
  my $self = bless(dbitoolbase->new(@_, name=>"streamlist"), $class);
}

# Locate a stream
sub search {
  my($self, $name)=@_;
  return($self->{list}->{$name});
}

# Instantiate a new stream (if needed) and return it
sub get {
  my($self, $name, $dir, $in_type, $out_type)=@_;
  my $stream = $self->{list}->{$name};
  if (! defined $stream) {
    $stream = dbitoolstreamobj->new(name=>$name, in_type=>$in_type, out_type=>$out_type, max=>$self->{max}, in_count=>0, out_count=>0);
    $self->{list}->{$name} = $stream;
  #} else { # TODO finish this test
  #  $self->die("cannot read and write to the same file ($stream->{name})") if ($
  }
  $stream->{$dir . "_count"} ++;
  return($dir eq "out" ? $stream : $stream->register);
}

# Check if all streams are linked
sub check {
  my($self)=@_;
  for my $k (keys %{$self->{list}}) {
    if ($k ne "error") {
      my $stream = $self->{list}->{$k};
      for my $dir ("in", "out") {
        $self->die("stream $stream->{name} is not connected to any module's ${dir}put") if ($stream->{$dir . "_count"} == 0);
      }
    }
  }
}

1;

package dbitoolstat;

# Class to manage module usage and statistics

use strict;

sub new {
  my($class)=shift;
  bless({@_}, $class);
}

# Add a stat
sub sum {
  my($self, $var, $n)=@_;
  $self->{$var} += $n;
}

# Summary of all stats in string
sub summary {
  my($self)=@_;
  my $s = "";
  for my $k (sort keys %{$self}) {
    my $val = sprintf(int($self->{$k}) == $self->{$k} ? "%d" : "%.3f", $self->{$k});
    $s .= sprintf("%s%s: %s", $s eq "" ? "" : ", ", $k, $val);
  }
  return($s);
}

1;

package dbitoolmod;

# Base class for all modules

use base 'dbitoolbase';
use Data::Dumper;
use JSON;
use IO::Select;
use if $main::debugTrace, "debugClass", qw/trace/;
use Carp qw/longmess croak/;
use strict;

sub args {
  return(
    name => "",                        # module name
    memorylimit => 1024,               # small limit for internal mods
    in_req => 1,                       # if in stream is required
    in_type => "",                     # type in stream type
    in => "",                          # name of input source
    in_stream => undef,                # ptr to input stramobj
    in_memory => 0,                    # 1 if module buffers everything
    out_req => 1,                      # is required
    out_type => "",                    # the out stream type
    out => "",                         # name of output destination
    out_stream => undef,               # link to the out stream
    savecol => 0,                      # write/save column names
    verbose => 0,                      # individual verbose opt
  );
}

sub new {
  my($class)=shift;
  my $self = bless(dbitoolbase->new(&dbitoolmod::args, @_, stats=>dbitoolstat->new), $class);
  $self->{memorylimit} = $self->{max} * 2 if ($self->{memorylimit} < $self->{max} * 2);
  return($self);
}

# Setup module parameters and streams
sub setup {
  my($self)=@_;
  $self->die("in stream type not defined") if ($self->{in_type} eq "");
  $self->die("out stream type not defined") if ($self->{out_type} eq "");
  my $args = delete($self->{args});
  map {
    $self->die("invalid argument \"$_\" in module $self->{name}") if (! $self->parse($_));
  } split(/:/, $args);
  $self->die("in stream required, and cannot be empty on first module") if ($main::streamLastOut eq "" && $self->{in} eq "" && $self->{in_req});
  if ($self->{in} eq "" && $self->{in_req}) {
    $self->usestream($main::streamLastOut, "in");
  }
  if ($self->{out} eq "" && $self->{out_req}) {
    $self->usestream("stream" . ($main::nextStream++), "out");
  }
  $self->die("in stream not specified") if ($self->{in_req} && $self->{in} eq "");
  $self->die("out stream not specified") if ($self->{out_req} && $self->{out} eq "");
  $main::streamLastOut = $self->{out} if ($self->{out} ne "" && defined $self->{out_stream});
}

# Parse module arguments
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(verbose)=([0-3])$/));
}

# Parse and set a variable
sub parsevar {
  my($self, $var, $new, $sub)=@_;
  return(0) if (! defined $var || ! defined $new);
  $self->{$var} = defined $sub ? &$sub : $new;
  return(1);
}


# Fetch a stream from the list and links it to this module
sub usestream {
  my($self, $name, $dir)=@_;
  my $stream = $main::streamlist->get($name, $dir, $self->{in_type}, $self->{out_type});
  $self->{$dir} = $stream->{name};
  $self->{$dir . "_stream"} = $stream;
  return($stream->{name});

}

# A string representation for logging
sub tostring {
  my($self)=@_;
  sprintf("%s (output %stype %s)", $self->{name}, (defined $self->{out_stream} ? "\"" . $self->{out_stream}->{name} . "\" " : ""), $self->{out_type});
}

# Open: get column names and create storage if needed
sub open {
  my($self)=@_;
  #$self->create if ($self->{create}); # TODO: check if needed
  &main::l($self->{name}, "open (in: " . ($self->{in} ne "" ? $self->{in} : "-") . ", out: " . ($self->{out} ne "" ? $self->{out} : "-") . ")") if ($self->{verbose} >= 1);
}

# Prepare the input column names: call getoutcolname and setcol
sub prepincolname {
  my($self)=@_;
  return if ($#{$self->{in_col}} >= 0 || ! defined $self->{in_stream});
  my $col = $self->{in_stream}->getoutcolname;
  if ($#{$col} >= 0) {
    $self->setcol("in", $col);
    &main::l($self->{name}, "reading columns: [" . join(",", @{$col}) . "]") if ($self->{verbose} >= 2);
  }
}

# Prepare the output column names: call getcol and setcol
sub prepoutcolname {
  my($self)=@_;
  return if ($#{$self->{out_col}} >= 0);
  $self->setcol("out", $self->{in_col});
  &main::l($self->{name}, "writing columns: [" . join(",", @{$self->{out_col}}) . "]") if ($self->{verbose} >= 2);
}

# Return true if handle has data to read
sub selectfh {
  my($self, $fh) = @_;
  my @ready = IO::Select->new($fh)->can_read(0);
  return($#ready >= 0);
}

# Set column names and calculate column idxs
sub setcol {
  my($self, $dir, $col)=@_;
  $self->{$dir . "_col"} = $col;
  if ($dir eq "in") {
    $self->{colidx} = {};
    my $i = 0;
    for my $c (@{$self->{in_col}}) {
      $self->{colidx}->{$c} = $i++;
    }
  } else {
    $self->{out_stream}->setcol("out", $col) if (defined $self->{out_stream});
  }
}

# Iterate the module: read and write
sub step {
  my($self)=@_;
  if (defined $self->{in_stream} && ! $self->{eof} && $self->{in_stream}->eof) {
    $self->seteof;
  } else {
    my $n = 0;
    while ($n < $self->{max} && $self->readok && $self->writeok) {
      my $data = $self->read;
      if (defined $data) {
        &main::l($self->{name}, Data::Dumper->new([$data], ['data'])->Indent(0)->Dump) if ($self->{verbose} >= 3 && ! $self->{log_stream});
        $self->{stats}->sum("rows", 1);
        $self->{stats}->sum("kB", length(join("", @{$data}))/1024) if ($self->{verbose} >= 3);
        $self->write($data);
        $n++;
        $self->{in_count} ++;
        $self->die("maximum number of in memory rows ($self->{memorylimit}) reached, stoping module $self->{name}") if ($self->{in_count} > $self->{memorylimit});
      } else {
        $self->seteof;
        return;
      }
    }
  }
}

# Return true if can read data
sub readok {
  my($self)=@_;
  return(0) if ($self->{eof});
  return($self->{in_stream}->canget);
}

# Read data
sub read {
  my($self)=@_;
  return($self->{in_stream}->get);
}

# Return true if can write data
sub writeok {
  my($self)=@_;
  return($self->{out_stream}->canput);
}

# Write data
sub write {
  my($self, $data)=@_;
  $self->{out_stream}->put($data);
}

# Set EOF for this module
sub seteof {
  my($self)=@_;
  if ($self->{verbose} >= 2) {
    &main::l($self->{name}, "close, " . $self->{stats}->summary);
  } elsif ($self->{verbose} >= 1) {
    &main::l($self->{name}, "close");
  }
  $self->{eof} = 1;
  $self->{out_stream}->{eof} = 1 if (defined $self->{out_stream});
}

# Convert array of data into a hash with column names as keys
sub mapcolname {
  my($self, $data)=@_;
  my $h = {};
  map { $h->{$_} = $data->[$self->{colidx}->{$_}] } @{$self->{in_col}};
  return($h);
}

# Convert a hash with column names to array of data
sub unmapcolname {
  my($self, $h)=@_;
  my $data;
  map { push @{$data},($h->{$_}); } @{$self->{out_col}};
  return($data);
}

# Close the storage
sub close { }

1;

package dbitoolmodlog;

# Module for logging

use base 'dbitoolmod';
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    name => "log",
    in_req => 0,
    in_type => "none",
    out_type => "row",
    out_streamname => "log",
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmodlog::args, @_), $class);
  $self->usestream($self->{out_streamname}, "out");
  $self->setcol("out", ["time","mod","msg"]);
  return($self);
}

# Iterate: do nothing
sub step { }

1;

package dbitoolmoderror;

# Module for logging errors

use base 'dbitoolmodlog';
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub write {
  my($self, $data)=@_;
  $self->SUPER::write($data);
  $main::log->write($data) if (defined $main::log && ! $main::log->{eof});
}

1;

package dbitoolmod_fileread;

# Module to read a file

use base 'dbitoolmod';
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "none",
    out_type => "raw",
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_fileread::args, @_), $class);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/)
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->SUPER::parse($arg));
}

sub tostring {
  my($self)=@_;
  sprintf("%s from file \"%s\"", $self->SUPER::tostring, $self->{in});
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{fh} = IO::File->new($self->{in}, "r") || $self->die("error opening $self->{fname}: $!");
  $self->setcol("in", ["col1"]);
  $self->setcol("out", ["col1"]);
}

sub readok {
  my($self)=@_;
  return($self->selectfh($self->{fh}));
}

sub read {
  my($self)=@_;
  my $line = $self->{fh}->getline;
  if (! defined $line) {
    $self->seteof;
    return(undef);
  }
  return([$line]);
}

sub close {
  my($self)=@_;
  $self->{fh}->close;
  $self->SUPER::close();
}

1;

package dbitoolmod_stdin;

# Module to read from STDIN

use base 'dbitoolmod_fileread';
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_req => 0,
    in_type => "none",
    out_type => "raw",
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod_fileread->new(&dbitoolmod_stdin::args, @_), $class);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->dbitoolmod::open;
  $self->{fh} = IO::Handle->new();
  $self->{fh}->fdopen(fileno(STDIN), "r") || $self->die("error reading from STDIN: $!");
  $self->setcol("out", ["col1"]);
}

1;

package dbitoolmod_stdout;

# Module to write to STDOUT

use base 'dbitoolmod';
use IO::Handle;
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "raw",
    out_req => 0,
    out_type => "none",
    eol => "\n",
    text => 1,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_stdout::args, @_), $class);
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(text)=([01])$/)
      || $self->parsevar($arg =~ /^(eol)=(.)$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{fh} = IO::Handle->new();
  $self->{fh}->fdopen(fileno(STDOUT), "w") || $self->die("error writing to STDOUT: $!");
  $self->{fh}->write([join(" ", @{$self->{col}}) . $self->{eol}]) if ($self->{savecol});
}

sub writeok {
  return(1);
}

sub write {
  my($self, $data)=@_;
  if ($self->{text}) {
    $self->{fh}->write(join(" ", @{$data}) . "\n");
  } else {
    $self->{fh}->write(join("", @{$data}));
  }
}

sub close {
  my($self)=@_;
  $self->{fh}->close;
  $self->SUPER::close();
}

1;

package dbitoolmod_stderr;

# Module to write to STDERR

use base 'dbitoolmod_stdout';
use IO::Handle;
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{fh} = IO::Handle->new();
  $self->{fh}->fdopen(fileno(STDERR), "w") || $self->die("error writing to STDERR: $!");
  $self->{fh}->write([join(" ", @{$self->{col}}) . $self->{eol}]) if ($self->{savecol});
}

1;

package dbitoolmod_filewrite;

# Module to write to a file

use base 'dbitoolmod_stdout';
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "raw",
    out_req => 0,
    out_type => "none",
    text => 0,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod_stdout->new(&dbitoolmod_filewrite::args, @_), $class);
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/)
      || $self->parsevar($arg =~ /^(text)=([01])$/)
      || $self->SUPER::parse($arg));
}

sub tostring {
  my($self)=@_;
  sprintf("%s to file \"%s\"", $self->SUPER::tostring, $self->{out});
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{fh} = IO::File->new($self->{out}, "w") || $self->die("error opening $self->{fname}: $!");
}

1;

package dbitoolmod_column;

# Module to select columns from the input

use base 'dbitoolmod';
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "row",
    out_type => "row",
    clist => "",
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_column::args, @_), $class);
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(clist)=(.+)$/)
      || $self->SUPER::parse($arg));
}

# Set the output columns acording to the args
sub prepoutcolname {
  my($self)=@_;
  $self->die("no column list specified (clist)") if ($self->{clist} eq "");
  my $clist = [split(/,/, $self->{clist})];
  $self->setcol("out", $clist);
  $self->{selidx} = [];
  map { push @{$self->{selidx}}, ($self->{colidx}->{$_}) } @{$clist};
  #print "column::colidx: " . Dumper($self->{colidx}) . "col: " . Dumper($clist) . "selidx: " . Dumper($self->{selidx});
  &main::l($self->{name}, "writing columns: [" . join(",", @{$self->{out_col}}) . "]") if ($self->{verbose} >= 2);
}

sub write {
  my($self, $data)=@_;
  my $d;
  map { push @{$d}, ($data->[$_]) } @{$self->{selidx}};
  $self->SUPER::write($d);
}

1;

package dbitoolmod_gzip;

use base 'dbitoolmod';
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "raw",
    out_type => "raw",
    level => 6,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_gzip::args, @_), $class);
  $self->requirepackage("IO::Compress::Gzip");
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(level)=(\d)$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{buf} = "";
  $self->{gzip} = IO::Compress::Gzip->new(\$self->{buf}, Append=>1, Minimal=>1, Level=>$self->{level}) || $self->die("error opening $self->{fname}: " . &IO::Compress::Gzip::GzipError);
  $self->setcol("out", ["col1"]);
}

sub writeok {
  my($self)=@_;
  return(1);
}

sub write {
  my($self, $data)=@_;
  $self->{gzip}->write($data->[0]);
  $self->flush;
}

sub flush {
  my($self)=@_;
  my $buf = $self->{buf};
  $self->{buf} = "";
  if ($buf ne "") {
    $self->{out_stream}->put([$buf]);
  }
}

sub seteof {
  my($self)=@_;
  $self->{gzip}->close;
  $self->flush;
  $self->SUPER::seteof;
}

1;

package dbitoolmod_gunzip;

use base 'dbitoolmod';
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "raw",
    out_type => "raw",
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_gunzip::args, @_), $class);
  $self->requirepackage("IO::Uncompress::Gunzip");
  $self->requirepackage("IO::String");
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  #print "gunzip::parse($arg)\n";
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{buf} = "";
  $self->setcol("out", ["col1"]);
}

sub writeok {
  return(1);
}

sub write {
  my($self, $data)=@_;
  $self->{buf} .= $data->[0];
  #print "gunzip::unc size: " . length($self->{buf}) . "\n";
  $self->flush;
}

sub flush {
  my($self)=@_;
  if (! defined $self->{gunzip} && length($self->{buf}) > 10) {
    $self->{iostring} = IO::String->new($self->{buf});
    $self->{gunzip} = IO::Uncompress::Gunzip->new($self->{iostring}) || $self->die("error opening gunzip: " . &IO::Uncompress::Gunzip::GunzipError);
    #print "gunzip instantiated!\n";
  }
  if (defined $self->{gunzip}) {
    my $unc;
    my $r = $self->{gunzip}->read($unc);
    #print "r: $r\ngunzip::unc: " . Dumper(\$unc);
    $self->{out_stream}->put([$unc]) if ($unc ne "");
  }
}

sub seteof {
  my($self)=@_;
  #print "gunzip::seteof\n";
  $self->flush;
  $self->{gunzip}->close if (defined $self->{gunzip});
  $self->SUPER::seteof;
}

1;

package dbitoolmoddbdread;

use base 'dbitoolmod';
use Data::Dumper;
use DBI;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    dbh => undef,
    sth => undef,
    db => "",
    query => "",
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmoddbdread::args, @_), $class);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(db)=(.+)$/)
      || $self->parsevar($arg =~ /^(query)=(.+)$/)
      || $self->SUPER::parse($arg));
}

sub tostring {
  my($self)=@_;
  sprintf("%s from database %s", $self->SUPER::tostring, $self->{db});
}

sub prepoutcolname {
  my($self)=@_;
  $self->{sth} = $self->{dbh}->prepare($self->{query});
  $self->{sth}->execute;
  my $ocol = $self->{sth}->{NAME};
  $self->setcol("out", $ocol);
}

sub readok {
  return(1);
}

sub read {
  my($self)=@_;
  my @row = $self->{sth}->fetchrow_array;
  if ($#row < 0) {
    return(undef);
  } else {
    return([@row]);
  }
}

sub close {
  my($self)=@_;
  $self->{sth}->finish;
  $self->{dbh}->disconnect;
  delete $self->{dbh};
  $self->SUPER::close;
}

1;

package dbitoolmod_mysqlselect;

use base 'dbitoolmoddbdread';
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_req => 0,
    in_type => "raw",
    out_type => "row",
    host => "localhost",
    port => 3306,
    user => "",
    pw => "",
    query => "",
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmoddbdread->new(&dbitoolmod_mysqlselect::args, @_), $class);
  $self->requirepackage("DBD::mysql");
  return($self);
}

sub tostring {
  my($self)=@_;
  sprintf("%s\@%s:%d", $self->SUPER::tostring, $self->{host}, $self->{port});
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(host)=(.+)$/)
      || $self->parsevar($arg =~ /^(port)=(\d+)$/)
      || $self->parsevar($arg =~ /^(user)=(.+)$/)
      || $self->parsevar($arg =~ /^(pw)=(.+)$/)
      || $self->parsevar($arg =~ /^(table)=(.+)$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->die("host not specified") if ($self->{host} eq "");
  $self->die("database (db) not specified") if ($self->{db} eq "");
  $self->die("query nor table nor input stream specified") if ($self->{query} eq "" && $self->{table} eq "" && $self->{in} eq "");
  $self->die("can specify only of query, table or input stream") if ((($self->{query} ne "") + ($self->{table} ne "") + ($self->{in} ne "")) > 1);
  $self->{query} = "SELECT * FROM $self->{table}" if ($self->{query} eq "" && $self->{table} ne "");
  $self->{dbh} = DBI->connect("dbi:mysql:host=$self->{host}:port=$self->{port}:database=$self->{db}:", $self->{user}, $self->{pw}, {'RaiseError' => 1, AutoCommit => 1})
    || $self->die("error connecting to MySQL at $self->{host}:$self->{port}");
}

sub prepoutcolname {
  my($self)=@_;
  if (defined $self->{in_stream}) {
    while (! $self->{in_stream}->eof && $self->{in_stream}->canget) {
      my $data = $self->{in_stream}->get;
      $self->{query} .= $data->[0] . " " if (defined $data);
    }
    return if (! $self->{in_stream}->eof);
    $self->{in_stream} = undef; # not needed anymore - TODO: in the future create a "module stream pool"
  }
  &main::l($self->{name}, "query: " . $self->{query}) if ($self->{verbose} > 2);
  $self->{sth} = $self->{dbh}->prepare($self->{query});
  $self->{sth}->execute;
  my $ocol = $self->{sth}->{NAME};
  $self->setcol("out", $ocol);
}

1;

package dbitoolmod_cassandraselect;

use base 'dbitoolmoddbdread';
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_req => 0,
    in_type => "raw",
    out_type => "row",
    host => "localhost",
    #port => 0,
    keyspace => "",
    consistency => "one",
    query => "",
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmoddbdread->new(&dbitoolmod_cassandraselect::args, @_), $class);
  $self->requirepackage("DBD::Cassandra");
  return($self);
}

sub tostring {
  my($self)=@_;
  sprintf("%s\@%s", $self->SUPER::tostring, $self->{host});
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(host)=(.+)$/)
      || $self->parsevar($arg =~ /^(keyspace)=(.+)$/)
      || $self->parsevar($arg =~ /^(consistency)=(.+)$/)
      || $self->parsevar($arg =~ /^(table)=(.+)$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->die("host not specified") if ($self->{host} eq "");
  $self->die("keyspace not specified") if ($self->{keyspace} eq "");
  $self->die("query nor table nor input stream specified") if ($self->{query} eq "" && $self->{table} eq "" && $self->{in} eq "");
  $self->die("can specify only of query, table or input stream") if ((($self->{query} ne "") + ($self->{table} ne "") + ($self->{in} ne "")) > 1);
  $self->{query} = "SELECT * FROM $self->{table}" if ($self->{query} eq "" && $self->{table} ne "");
  $self->{dbh} = DBI->connect("dbi:Cassandra:host=$self->{host};keyspace=$self->{keyspace}", "", "", { RaiseError => 1 })
    || $self->die("error connecting to Cassandra at $self->{host}");
}

sub prepoutcolname {
  my($self)=@_;
  if (defined $self->{in_stream}) {
    while (! $self->{in_stream}->eof && $self->{in_stream}->canget) {
      my $data = $self->{in_stream}->get;
      $self->{query} .= $data->[0] . " " if (defined $data);
    }
    return if (! $self->{in_stream}->eof);
    $self->{in_stream} = undef; # not needed anymore - TODO: in the future create a "module stream pool"
  }
  &main::l($self->{name}, "query: " . $self->{query}) if ($self->{verbose} > 2);
  $self->{sth} = $self->{dbh}->prepare($self->{query}, {Consistency => $self->{consistency}});
  $self->{sth}->execute;
  my $ocol = $self->{sth}->{NAME};
  $self->setcol("out", $ocol);
}

1;

package dbitoolmod_sqliteselect;

use base 'dbitoolmoddbdread';
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_req => 0,
    in_type => "raw",
    out_type => "row",
    fname => "",
    query => "",
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmoddbdread->new(&dbitoolmod_sqliteselect::args, @_), $class);
  $self->requirepackage("DBD::SQLite");
  return($self);
}

sub tostring {
  my($self)=@_;
  sprintf("%s\@%s", $self->SUPER::tostring, $self->{fname});
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(fname)=(.+)$/)
      || $self->parsevar($arg =~ /^(table)=(.+)$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->die("SQLite db (fname) not specified") if ($self->{fname} eq "");
  $self->die("query nor table nor input stream specified") if ($self->{query} eq "" && $self->{table} eq "" && $self->{in} eq "");
  $self->die("can specify only of query, table or input stream") if ((($self->{query} ne "") + ($self->{table} ne "") + ($self->{in} ne "")) > 1);
  $self->{query} = "SELECT * FROM $self->{table}" if ($self->{query} eq "" && $self->{table} ne "");
  $self->{dbh} = DBI->connect("dbi:SQLite:dbname=$self->{fname}", $self->{user}, $self->{pw})
    || $self->die("error connecting to SQLite in $self->{fname}");
}

sub prepoutcolname {
  my($self)=@_;
  if (defined $self->{in_stream}) {
    while (! $self->{in_stream}->eof && $self->{in_stream}->canget) {
      my $data = $self->{in_stream}->get;
      $self->{query} .= $data->[0] . " " if (defined $data);
    }
    return if (! $self->{in_stream}->eof);
    $self->{in_stream} = undef; # not needed anymore - TODO: in the future create a "module stream pool"
  }
  &main::l($self->{name}, "query: " . $self->{query}) if ($self->{verbose} > 2);
  $self->{sth} = $self->{dbh}->prepare($self->{query});
  $self->{sth}->execute;
  my $ocol = $self->{sth}->{NAME};
  $self->setcol("out", $ocol);
}

1;

package dbitoolmoddbdwrite;

use base 'dbitoolmod';
use Data::Dumper;
use DBI;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "row",
    out_req => 0,
    out_type => "none",
    dbh => undef,
    sth => undef,
    db => "",
    query => "",
    dbhprepareopt => {},               # options (hash) to pass to $dbh->prepare
    sthqueuesize => 1024,              # number of sth handles in queue
    sthreuse => [],                    # sth reuse queue
    sthpending => [],                  # sth insert pending queue
    sthwaittime => 0,                  # total wait time in seconds
    sthwaitcount => 0,                 # number of times had to wait insert
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmoddbdwrite::args, @_), $class);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(db)=(.+)$/)
      || $self->parsevar($arg =~ /^(query)=(.+)$/)
      || $self->SUPER::parse($arg));
}

sub tostring {
  my($self)=@_;
  sprintf("%s to database %s", $self->SUPER::tostring, $self->{db});
}

#sub prepoutcolname {
#  my($self)=@_;
#  $self->{sth} = $self->{dbh}->prepare($self->{query});
#  $self->{sth}->execute;
#  my $ocol = $self->{sth}->{NAME};
#  $self->setcol("out", $ocol);
#}

sub write {
  my($self, $data)=@_;
  my $sth = $self->nextsth;
  $sth->execute(@{$data});
}

# Get the next sth handle for inserting using async queries
sub nextsth {
  my($self)=@_;
  my $sth = (shift @{$self->{sthreuse}}) || $self->{dbh}->prepare($self->{query}, $self->{dbhprepareopt});
  if ($#{$self->{sthpending}} > $self->{sthqueuesize}) {
    # flush pending inserts if reached max queue size
    my $pending_sth = shift @{$self->{sthpending}};
    #my $time0 = [gettimeofday];
    $self->flushsth($pending_sth);
    #$pending_sth->x_finish_async;
    #$self->{sthwaittime} += tv_interval($time0, [gettimeofday]);
    $self->{sthwaitcount}++;
    push @{$self->{sthreuse}}, ($pending_sth);
  }
  push @{$self->{sthpending}}, ($sth);
  return($sth);
}

# Flush a sth handle
sub flushsth {
  my($self, $sth)=@_;
  $self->die("abstract call");
}

sub close {
  my($self)=@_;
  $self->flushsth($_) for @{$self->{sthpending}};
  $self->{sth}->finish;
  $self->{dbh}->disconnect;
  delete $self->{dbh};
  $self->SUPER::close;
}

1;

package dbitoolmod_sqliteinsert;

use base 'dbitoolmoddbdwrite';
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    fname => "",
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmoddbdwrite->new(&dbitoolmod_sqliteinsert::args, @_), $class);
  $self->requirepackage("DBD::SQLite");
  return($self);
}

sub tostring {
  my($self)=@_;
  sprintf("%s\@%s", $self->SUPER::tostring, $self->{fname});
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(fname)=(.+)$/)
      #|| $self->parsevar($arg =~ /^(table)=(.+)$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->die("SQLite db (fname) not specified") if ($self->{fname} eq "");
  $self->die("query not specified") if ($self->{query} eq "");
  $self->{dbh} = DBI->connect("dbi:SQLite:dbname=$self->{fname}", $self->{user}, $self->{pw})
    || $self->die("error connecting to SQLite in $self->{fname}");
}

#sub prepoutcolname {
#  my($self)=@_;
#  if (defined $self->{in_stream}) {
#    while (! $self->{in_stream}->eof && $self->{in_stream}->canget) {
#      my $data = $self->{in_stream}->get;
#      $self->{query} .= $data->[0] . " " if (defined $data);
#    }
#    return if (! $self->{in_stream}->eof);
#    $self->{in_stream} = undef; # not needed anymore - TODO: in the future create a "module stream pool"
#  }
#  &main::l($self->{name}, "query: " . $self->{query}) if ($self->{verbose} > 2);
#  $self->{sth} = $self->{dbh}->prepare($self->{query});
#  $self->{sth}->execute;
#  my $ocol = $self->{sth}->{NAME};
#  $self->setcol("out", $ocol);
#}

1;

package dbitoolmod_csvread;

use base 'dbitoolmod';
use Data::Dumper;
use Cwd 'abs_path';
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "raw",
    out_type => "row",
    eol => "\n",
    sep => ",",
    quote => "",
    escape => "",
    header => 1,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_csvread::args, @_), $class);
  $self->requirepackage("Text::CSV");
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(eol)=(.)$/)
      || $self->parsevar($arg =~ /^(quote)=(.)$/)
      || $self->parsevar($arg =~ /^(escape)=(.)$/)
      || $self->parsevar($arg =~ /^(header)=([01])$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{csv} = Text::CSV->new({binary=>1, sep=>$self->{sep}, quote=>$self->{quote}, escape_char=>$self->{escape}}) || $self->die("cannot use Text::CSV: ".Text::CSV->error_diag ());
}

sub prepoutcolname {
  my($self)=@_;
  if ($self->{in_stream}->canget) {
    my $data = $self->{in_stream}->get;
    $self->{csv}->column_names($self->{csv}->parse($data->[0]));
    my $ocol = [$self->{csv}->fields()];
    map { if (/^"([^"]+)"/) { $_ = $1; } } @{$ocol}; # TODO: why Text::CSV keeps quotes?
    $self->setcol("out", $ocol);
  }
}

sub read {
  my($self)=@_;
  my $line = $self->{in_stream}->get;
  $self->{csv}->parse($line->[0]);
  my $data = [$self->{csv}->fields()];
  map { if (/^"([^"]+)"/) { $_ = $1; } } @{$data}; # TODO: why Text::CSV keeps quotes?
  return($data);
}

1;

package dbitoolmod_csvwrite;

use base 'dbitoolmod';
use Data::Dumper;
use Cwd 'abs_path';
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "row",
    out_type => "raw",
    create => 1,
    overwrite => 1,
    eol => "\n",
    sep => ",",
    quote => "",
    escape => "",
    header => 1,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_csvwrite::args, @_), $class);
  $self->requirepackage("Text::CSV");
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(eol)=(.)$/)
      || $self->parsevar($arg =~ /^(sep)=(.)$/)
      || $self->parsevar($arg =~ /^(quote)=(.)$/)
      || $self->parsevar($arg =~ /^(escape)=(.)$/)
      || $self->parsevar($arg =~ /^(header)=([01])$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{csv} = Text::CSV->new({binary=>1, sep=>$self->{sep}, quote=>$self->{quote}, escape_char=>$self->{escape}}) || $self->die("cannot use Text::CSV: ".Text::CSV->error_diag ());
  $self->setcol("out", ["col1"]);
}

sub setcol {
  my($self, $dir, $col)=@_;
  $self->SUPER::setcol($dir, $col);
  if ($dir eq "in") {
    $self->{csv}->column_names(@{$self->{in_col}});
    if ($self->{header}) {
      $self->{csv}->combine(@{$self->{in_col}});
      $self->write([$self->{csv}->string()]);
    }
  }
}

sub write {
  my($self, $data)=@_;
  $self->{csv}->combine(@{$data});
  my $s = $self->{csv}->string();
  $self->{out_stream}->put([$s]);
}

1;

package dbitoolmod_fixedwidthread;

use base 'dbitoolmod';
use Data::Dumper;
use Cwd 'abs_path';
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "raw",
    out_type => "row",
    width => "",
    header => 1,
    trim => 1,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_fixedwidthread::args, @_), $class);
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(width)=([\d,]+)$/)
      || $self->parsevar($arg =~ /^(header)=([01])$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->die("width not specified") if ($self->{width} eq "");
  $self->{widthcol} = [split(/,/, $self->{width})];
}

sub prepoutcolname {
  my($self)=@_;
  my $ocol = [];
  if ($self->{header}) {
    if ($self->{in_stream}->canget) {
      my $data = $self->{in_stream}->get;
      $ocol = $self->splitfixed($data->[0]);
      $self->setcol("out", $ocol);
    }
  } else {
    my $i = 1;
    for my $w (@{$self->{widthcol}}) {
      push @{$ocol}, ("col$i");
      $i++;
    }
    $self->setcol("out", $ocol);
  }
}

sub read {
  my($self)=@_;
  my $data = $self->{in_stream}->get;
  return($self->splitfixed($data->[0]));
}

sub splitfixed {
  my($self, $s)=@_;
  my $d = [];
  my $p = 0;
  for my $w (@{$self->{widthcol}}) {
    my $f = substr($s, $p, $w);
    ($f) = $f =~ /^\s*(.*[^\s])\s*$/ if ($self->{trim});
    push @{$d}, ($f);
    $p += $w;
  }
  return($d);
}

1;

package dbitoolmod_fixedwidthwrite;

use base 'dbitoolmod';
use Data::Dumper;
use Cwd 'abs_path';
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "row",
    out_type => "raw",
    width => "",
    eol => "\n",
    header => 1,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_fixedwidthwrite::args, @_), $class);
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(width)=([\d,]+)$/)
      || $self->parsevar($arg =~ /^(eol)=(.)$/)
      || $self->parsevar($arg =~ /^(header)=([01])$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->die("width not specified") if ($self->{width} eq "");
  $self->{widthcol} = [split(/,/, $self->{width})];
  $self->setcol("out", ["col1"]);
}

sub setcol {
  my($self, $dir, $col)=@_;
  $self->SUPER::setcol($dir, $col);
  if ($dir eq "in") {
    $self->die("number of widths ($#{$self->{widthcol}}) differ from number of columns ($#{$col})") if ($#{$col} != $#{$self->{widthcol}});
    $self->write($col) if ($self->{header});
  }
}

sub write {
  my($self, $data)=@_;
  $self->{out_stream}->put([$self->format($data)]);
}

sub format {
  my($self, $data)=@_;
  my $s = "";
  for (my $i = 0; $i <= $#{$data}; $i++) {
    $s .= substr(sprintf("%-" . $self->{widthcol}->[$i] . "s", $data->[$i]), 0, $self->{widthcol}->[$i]);
  }
  return($s);
}

1;

package dbitoolmod_xmlread;

use base 'dbitoolmod';
use IO::File;
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "raw",
    in_memory => 1,
    out_type => "row",
    root => "xmlarray",
    row => "row",
    trim => 0,
    attrprefix => "",
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_xmlread::args, @_), $class);
  $self->requirepackage("XML::Fast");
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(root)=(.+)$/)
      || $self->parsevar($arg =~ /^(row)=(.+)$/)
      || $self->parsevar($arg =~ /^(trim)=([01])$/)
      || $self->parsevar($arg =~ /^(attrprefix)=(.+)$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{buf} = "";
}

sub prepoutcolname {
  my($self)=@_;
  while ($self->{in_stream}->canget) {
    my $data = $self->{in_stream}->get;
    $self->{buf} .= $data->[0];
  }
  if ($self->{in_stream}->eof) {
    $self->{data} = &XML::Fast::xml2hash($self->{buf}, attr=>$self->{attrprefix}, text=>"#text", trim=>$self->{trim}, array=>["node",$self->{row}]);
    $self->die("no root node \"$self->{root}\" found in XML") if (! exists $self->{data}->{$self->{root}});
    $self->die("no row node \"$self->{row}\" found in XML") if (! exists $self->{data}->{$self->{root}}->{$self->{row}});
    $self->{data} = $self->{data}->{$self->{root}}->{$self->{row}};
    my $ocol = [];
    map { push @{$ocol}, ($_); } (keys %{$self->{data}->[0]});
    $self->setcol("out", $ocol);
    $self->{idx} = 0;
    $self->{in_stream} = undef; # not needed anymore - TODO: in the future create a "module stream pool"
  }
}

sub readok {
  my($self)=@_;
  $self->seteof if ($self->{idx} > $#{$self->{data}});
  return($self->{idx} <= $#{$self->{data}});
}

sub read {
  my($self)=@_;
  if ($self->{idx} > $#{$self->{data}}) {
    $self->seteof;
    return(undef);
  }
  return($self->unmapcolname($self->{data}->[$self->{idx}++]));
}

1;

package dbitoolmod_xmlwrite;

use base 'dbitoolmod';
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "row",
    out_type => "raw",
    root => "xmlarray",
    row => "row",
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_xmlwrite::args, @_), $class);
  $self->requirepackage("XML::Fast");
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(root)=(.+)$/)
      || $self->parsevar($arg =~ /^(row)=(.+)$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{data} = [];
  $self->setcol("out", ["col1"]);
  $self->{out_stream}->put(["<" . $self->{root} . ">"]);
}

sub write {
  my($self, $data)=@_;
  my $s = "<$self->{row}>";
  map { $s .= "<$_>" . $self->escape($data->[$self->{colidx}->{$_}]) . "</$_>" } @{$self->{in_col}};
  $self->{out_stream}->put([$s . "</$self->{row}>"]);
}

sub seteof {
  my($self)=@_;
  $self->{out_stream}->put(["</" . $self->{root} . ">"]);
  $self->SUPER::seteof;
}

sub escape {
  my($self, $s)=@_;
  $s =~ s/\&/\&emp;/gs;
  $s =~ s/\</\&lt;/gs;
  $s =~ s/\>/\&gt;/gs;
  return($s);
}

1;

package dbitoolmod_jsonread;

use base 'dbitoolmod';
use Data::Dumper;
use JSON;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "raw",
    in_memory => 1,
    out_type => "row",
    utf8 => 1,
    pretty => 0,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_jsonread::args, @_), $class);
  $self->{json} = JSON->new->utf8($self->{utf8})->pretty($self->{pretty});
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(utf8)=([01])$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{buf} = "";
}

sub prepoutcolname {
  my($self)=@_;
  while ($self->{in_stream}->canget) {
    my $data = $self->{in_stream}->get;
    $self->{buf} .= $data->[0];
  }
  if ($self->{in_stream}->eof) {
    $self->{data} = $self->{json}->decode($self->{buf});
    my $ocol = [];
    map { push @{$ocol}, ($_); } (keys %{$self->{data}->[0]});
    $self->setcol("out", $ocol);
    $self->{idx} = 0;
    $self->{in_stream} = undef; # not needed anymore - TODO: in the future create a "module stream pool"
  }
}

sub readok {
  my($self)=@_;
  $self->seteof if ($self->{idx} > $#{$self->{data}});
  return($self->{idx} <= $#{$self->{data}});
}

sub read {
  my($self)=@_;
  if ($self->{idx} > $#{$self->{data}}) {
    $self->seteof;
    return(undef);
  }
  return($self->unmapcolname($self->{data}->[$self->{idx}++]));
}

1;

package dbitoolmod_jsonwrite;

use base 'dbitoolmod';
use JSON;
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "row",
    in_memory => 1,
    out_type => "raw",
    utf8 => 1,
    pretty => 0,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_jsonwrite::args, @_), $class);
  $self->{json} = JSON->new->utf8($self->{utf8})->pretty($self->{pretty});
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(utf8)=([01])$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{data} = [];
  $self->setcol("out", ["col1"]);
}

sub write {
  my($self, $data)=@_;
  push @{$self->{data}},($self->mapcolname($data));
}

sub seteof {
  my($self)=@_;
  $self->{out_stream}->put([$self->{json}->encode($self->{data})]);
  $self->SUPER::seteof;
}

1;

package dbitoolmod_ndjsonread;

use base 'dbitoolmod';
use IO::File;
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "raw",
    out_type => "row",
    newline => "\n",
    utf8 => 1,
    pretty => 0,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_ndjsonread::args, @_), $class);
  $self->requirepackage("JSON");
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(newline)=(.)$/)
      || $self->parsevar($arg =~ /^(utf8)=([01])$/)
      || $self->parsevar($arg =~ /^(pretty)=([01])$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{json} = JSON->new->utf8($self->{utf8})->pretty($self->{pretty});
}

sub prepoutcolname {
  my($self)=@_;
  if ($self->{in_stream}->canget) {
    my $data = $self->{in_stream}->get;
    $self->die("input NDJSON file is empty") if (! defined $data);
    my $row = $self->{json}->decode($data->[0]);
    $self->die("a NDJSON input has to be a file of one line hashes") if (ref($row) ne "HASH");
    my $ocol = [keys %{$row}];
    $self->setcol("out", $ocol);
    $self->write($self->unmapcolname($row));
  }
}

sub read {
  my($self)=@_;
  my $data = $self->{in_stream}->get;
  my $row = $self->unmapcolname($self->{json}->decode($data->[0]));
  return($row);
}

1;

package dbitoolmod_ndjsonwrite;

# Newline Delimited JSON writer module

use base 'dbitoolmod';
use IO::File;
use JSON;
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "row",
    out_type => "raw",
    newline => "\n",
    utf8 => 1,
    pretty => 0,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_ndjsonwrite::args, @_), $class);
  $self->{json} = JSON->new->utf8($self->{utf8})->pretty($self->{pretty});
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(eol)=(.)$/)
      || $self->parsevar($arg =~ /^(quote)=(.)$/)
      || $self->parsevar($arg =~ /^(escape)=(.)$/)
      || $self->parsevar($arg =~ /^(header)=([01])$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->setcol("out", ["col1"]);
}

sub write {
  my($self, $data)=@_;
  my $ndata = [$self->{json}->encode($self->mapcolname($data)) . $self->{newline}];
  $self->SUPER::write($ndata);
}

sub close {
  my($self)=@_;
  $self->SUPER::close();
}

1;

package dbitoolmod_bsonread;

use base 'dbitoolmod';
use IO::File;
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "raw",
    in_memory => 1,
    out_type => "row",
    utf8 => 1,
    pretty => 0,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_bsonread::args, @_), $class);
  $self->requirepackage("BSON");
  $self->{bson} = BSON->new;
  return($self);
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{buf} = "";
}

sub prepoutcolname {
  my($self)=@_;
  if ($self->{in_stream}->canget) {
    my $data = $self->{in_stream}->get;
    $self->{buf} .= $data->[0];
    if ($self->{in_stream}->eof) {
      my $a = $self->{bson}->decode_one($self->{buf}); # BSON doc has to be a HASH
      $self->{data} = $a->{a};
      $self->die("a BSON input has to be an array of hashes") if (ref($self->{data}) ne "ARRAY");
      my $ocol = [];
      map { push @{$ocol}, ($_); } (keys %{$self->{data}->[0]});
      $self->setcol("out", $ocol);
      $self->{idx} = 0;
    }
  }
}

sub readok {
  my($self)=@_;
  $self->seteof if ($self->{idx} > $#{$self->{data}});
  return($self->{idx} <= $#{$self->{data}});
}

sub read {
  my($self)=@_;
  if ($self->{idx} > $#{$self->{data}}) {
    $self->seteof;
    return(undef);
  }
  return($self->unmapcolname($self->{data}->[$self->{idx}++]));
}

1;

package dbitoolmod_bsonwrite;

use base 'dbitoolmod';
use IO::File;
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "row",
    in_memory => 1,
    out_type => "raw",
    newline => "\n",
    utf8 => 1,
    pretty => 0,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_bsonwrite::args, @_), $class);
  $self->requirepackage("BSON");
  $self->{bson} = BSON->new;
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(newline)=(.)$/)
      || $self->parsevar($arg =~ /^(utf8)=([01])$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{data} = [];
  $self->setcol("out", ["col1"]);
}

sub write {
  my($self, $data)=@_;
  push @{$self->{data}},($self->mapcolname($data));
}

sub seteof {
  my($self)=@_;
  $self->{out_stream}->put([$self->{bson}->encode_one({a=>$self->{data}})]);
  $self->SUPER::seteof;
}

1;

package dbitoolmod_ndb;

use base 'dbitoolmod';
use DB_File;
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "row",
    out_type => "row",
    key => "",
    keyidx => -1,
    idx => 0,
    savecol => 1,
    perm => 0644,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_ndb::args, @_), $class);
  $self->requirepackage("JSON");
  $self->{json} = JSON->new->utf8(1)->pretty(0);
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(fname)=(.+)$/)
      || $self->parsevar($arg =~ /^(key)=(.+)$/)
      || $self->parsevar($arg =~ /^(keyidx)=(\d+)$/)
      || $self->parsevar($arg =~ /^(perm)=(\d+)$/)
      || $self->parsevar($arg =~ /^(savecol)=([01])$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->die("cannot specify both key and keyidx in NDB") if ($self->{key} ne "" && $self->{keyidx} >= 0);
  $self->SUPER::open;
  tie(%{$self->{db}}, "DB_File", $self->{fname}, O_CREAT|O_RDWR, $self->{perm}, $DB_HASH) || $self->die("error writing to $self->{fname}: $!");
  $self->die("key or keyidx not specified") if ($self->{key} eq "" && $self->{keyidx} == -1);
  $self->{keys} = [keys %{$self->{db}}];
  if (! defined $self->{in_stream}) {
    #die("NDB should have \"_col\" entry with an array of columns when it is the first module") if (! exists $self->{db}->{_col});
    if (exists $self->{db}->{_col}) {
      $self->{out_col} = $self->{json}->decode($self->{db}->{_col});
    } else {
      $self->die("cannot read column names from empty NDB when it is the first module") if ($#{$self->{keys}} < 0);
      $self->die("cannot use keyidx when reading column names from JSON inside NDB") if ($self->{keyidx} >= 0);
      my $d = $self->{json}->decode($self->{db}->{$self->{keys}->[0]});
      for my $k (keys %{$d}) {
        push @{$self->{out_col}}, ($k);
      }
    }
    $self->setcol("out", $self->{out_col});
  }
}

# Set column names and calculate column idxs
sub setcol {
  my($self, $dir, $col)=@_;
  $self->SUPER::setcol($dir, $col);
  if ($dir eq "in" && $self->{savecol} && ! exists $self->{db}->{_col}) {
    $self->{db}->{_col} = $self->{json}->encode($self->{in_col});
    if ($self->{keyidx} < 0) {
      my $n = 0;
      map {
        $self->{keyidx} = $n if ($self->{key} eq $_);
        $n++;
      } @{$self->{in_col}};
      $self->die("key \"$self->{key}\" not found in input columns: (" . join(", ", @{$self->{col}}) . ")") if ($self->{keyidx} < 0);
    }
  }
}

sub readok {
  my($self)=@_;
  if (defined $self->{in_stream}) {
    return($self->SUPER::readok);
  } else {
    return($self->{idx} <= $#{$self->{keys}});
  }
}

sub read {
  my($self)=@_;
  if (defined $self->{in_stream}) {
    return($self->SUPER::read);
  } else {
    my $data;
    $self->{idx}++ if ($self->{idx} <= $#{$self->{keys}} && $self->{keys}->[$self->{idx}] eq "_col");
    if ($self->{idx} > $#{$self->{keys}}) {
      $self->seteof;
    } else {
      $data = $self->{json}->decode($self->{db}->{$self->{keys}->[$self->{idx}++]});
    }
    return($data);
  }
}

# Return true if can write data
sub writeok {
  my($self)=@_;
  if (defined $self->{out_stream}) {
    return($self->{out_stream}->canput);
  } else {
    return(1);
  }
}

sub write {
  my($self, $data)=@_;
  if (defined $self->{in_stream}) { # write to NDB only if data came from a stream
    my $k = $data->[$self->{keyidx}];
    $self->{db}->{$k} = $self->{json}->encode($data);
  }
  if (defined $self->{out_stream}) {
    $self->SUPER::write($data);
  }
}

sub close {
  my($self)=@_;
  untie $self->{db};
  $self->SUPER::close();
}

1;

package dbitoolmod_spreadsheetread;

use base 'dbitoolmod';
use IO::File;
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_req => 0,
    in_type => "raw",
    in_memory => 1,
    out_type => "row",
    fname => "",
    type => "", # xls, xlsx, csv, sxc or ods
    sheet => "",
    header => 1,
    x => 1,
    y => 1,
    utf8 => 1,
    pretty => 0,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_spreadsheetread::args, @_), $class);
  $self->requirepackage("Spreadsheet::Read");
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(out)=(.+)$/, sub {$self->usestream($2, "out")})
      || $self->parsevar($arg =~ /^(fname)=(.+)$/)
      || $self->parsevar($arg =~ /^(type)=(\w+)$/)
      || $self->parsevar($arg =~ /^(sheet)=(.+)$/)
      || $self->parsevar($arg =~ /^(header)=([01])$/)
      || $self->parsevar($arg =~ /^(x)=(\d+)$/)
      || $self->parsevar($arg =~ /^(y)=(\d+)$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{book} = Spreadsheet::Read->new($self->{fname}, cells=>0, clip=>1, strip=>3); # TODO: use type
  print Dumper($self->{book}) if ($main::debugTrace);
  $self->die("error parsing spreadsheet in $self->{fname}") if (! defined $self->{book});
}

sub prepoutcolname {
  my($self)=@_;
  $self->{sheet} = $self->{book}->sheet(1) if ($self->{sheet} eq "");
  $self->{sheetptr} = $self->{book}->sheet($self->{sheet});
  $self->die("sheet \"$self->{sheet}\" not found in $self->{fname}") if (! defined $self->{sheetptr});
  my $ocol = [];
  $self->{yofs} = 0;
  if ($self->{header}) {
    for (my $i = $self->{x}; $i <= $self->{sheetptr}->maxcol; $i++) {
      push @{$ocol}, $self->{sheetptr}->cell($i, $self->{y});
    }
    $self->{yofs} ++;
  } else {
    for (my $i = 1; $i <= $self->{sheetptr}->maxcol; $i++) {
      push @{$ocol}, ("col$i");
    }
  }
  $self->setcol("out", $ocol);
}

sub readok {
  my($self)=@_;
  $self->seteof if ($self->{y} + $self->{yofs} > $self->{sheetptr}->maxrow);
  return($self->{y} + $self->{yofs} <= $self->{sheetptr}->maxrow);
}

sub read {
  my($self)=@_;
  if ($self->{y} + $self->{yofs} > $self->{sheetptr}->maxrow) {
    $self->seteof;
    return(undef);
  }
  return([$self->{sheetptr}->cellrow($self->{y} + ($self->{yofs} ++))]);
}

1;

package dbitoolmod_spreadsheetwrite;

use base 'dbitoolmod';
use Data::Dumper;
use if $main::debugTrace, "debugClass", qw/trace/;
use strict;

sub args {
  return(
    in_type => "row",
    in_memory => 1,
    out_req => 0,
    out_type => "none",
    fname => "",
    #type => "", # TODO: implement other types
    sheet => "",
    header => 1,
    x => 1,
    y => 1,
  );
}

sub new {
  my $class=shift;
  my $self = bless(dbitoolmod->new(&dbitoolmod_spreadsheetwrite::args, @_), $class);
  $self->requirepackage("Spreadsheet::WriteExcel");
  return($self);
}

# Parse module parameters and streams
sub parse {
  my($self, $arg)=@_;
  return($self->parsevar($arg =~ /^(in)=(.+)$/, sub {$self->usestream($2, "in")})
      || $self->parsevar($arg =~ /^(fname)=(.+)$/)
      || $self->parsevar($arg =~ /^(sheet)=(.+)$/)
      || $self->parsevar($arg =~ /^(header)=([01])$/)
      || $self->parsevar($arg =~ /^(x)=(\d+)$/)
      || $self->parsevar($arg =~ /^(y)=(\d+)$/)
      || $self->SUPER::parse($arg));
}

sub open {
  my($self)=@_;
  $self->SUPER::open;
  $self->{workbook} = Spreadsheet::WriteExcel->new($self->{fname});
  $self->{workbook}->compatibility_mode(); # as suggested
  $self->{workbook}->set_properties(comments => 'Created with Perl, Spreadsheet::WriteExcel and dbitool');
  $self->{sheetptr} = $self->{workbook}->add_worksheet($self->{sheet});
  $self->{yofs} = 0;
}

sub setcol {
  my($self, $dir, $col)=@_;
  $self->SUPER::setcol($dir, $col);
  if ($dir eq "in") {
    if ($self->{header}) {
      my $i = 0;
      for my $c (@{$col}) {
        $self->{sheetptr}->write($self->{y} - 1, $self->{x} - 1 + ($i++), $c);
      }
      $self->{yofs} ++;
    }
  }
}

# Return true if can write data
sub writeok {
  return(1);
}

sub write {
  my($self, $data)=@_;
  for (my $i = 0; $i <= $#{$data}; $i++) {
    $self->{sheetptr}->write($self->{y} - 1 + $self->{yofs}, $self->{x} - 1 + $i, $data->[$i]);
  }
  $self->{yofs} ++;
}

sub close {
  my($self)=@_;
  $self->{workbook}->close;
  $self->SUPER::close();
}

1;

__END__

=head1 NAME

dbitool - Process data flow from different sources and destinations using DBI classes

=head1 SYNOPSIS

  dbitool [--listmodules|l] [--streamsize|s=N] [--errorsize|e=N]
    [--memorylimit|m=N] [--loglevel|l=N] [--verbose|v] [--help|h]
    module1 module2 ... modulen

  dbitool sets up a tree of sources and destination data modules to perform
  extract, transform and load. Serveral data formats, sources and
  destinations are available.

=head1 OPTIONS

Options:

  --listmodules
  -l
      List all avilable modules and for each one if its source or
      destination, data type and if in memory buffer is required. This
      option is mutually exclusive to all other options.

  --streamsize=N
  -s N
      Sets the number of rows read/writen in each interaction. Bigger values
      means fastest ETLs and a bigger memory footprint, after a certain
      threashold.

  --errorsize=N
  -e N
      Set the error limit. After the limit is reached, modules will be
      terminated.

  --memorylimit=N
  -m N
      Limits the number of rows to be kept in a memory buffer for in memory
      modules (json, bson, etc). dbitool will abort if the limit is
      reached. Use this limit to avoid using the OS swap memory.

  --loglevel=N
  -l N
      Set the log level (0=none, 3=debug) if -v is not used. This options is
      redundant if one or more -v is used.

  --verbose
  -v
      Set verbose mode. Cannot be used when using STDOUT module. Can be used
      up to three times to set verbose to 1 (normal), 2 (stats) or 3 (debug)
      levels.
  
  --help
  -h
      Show this help.

=head1 DESCRIPTION

Dbitool links together different data sources and destinations in a tree
structure and manages the data flow thru all modules until EOF has been
reached in all input modules.

Between modules, stream objects are instantiated to buffer data from one
module to another. Different modules and read from the same stream, hence
the tree structure. Data can be read or writen from/to databases, files,
spreadsheets or stdin. But two modules cannot output to the same stream.

Every operation can be logged and errors can be tracked for analysis. The
logging and error are also modules in dbitool and can be included in the
tree structure to be saved in any format handled by the program.

Each module has different arguments to fine tune how data is read or writen.
Some arguments are more global and some are specific to each data format,
source or destination.

Modules are specified in the command line and arguments are separated by a
collon (:). Each argument takes a name and a value separated by equal sign,
for example: fname=text.txt. The first argument is the module class, that
can be listed by running "dbitool --listmodules".

Some modules have to buffer data until EOF has been reached to process the
data. This is generally a pre-requisite of the data format like JSON. In a
JSON file, all rows have to be in memory for the JSON file to be created.
This is why some streaming software use newline delimited JSON (or NDJSON)
to read data from text files, like Apache Spark. Instead of buffering the
whole input file into memory and then parsing the JSON, NDJSON allows each
line of the input file to be read separatelly and streaming of data to start
with the first line, avoiding memory buffering. For big amounts of data,
avoid in memory modules. The XML write module does not use a XML class to
write the data, and therefore is not a "in memory" module. In the future,
jsonwrite module could also use this solution to avoid memory buffers.

=head1 LIST MODULES

The --listmodules option cannot be used with any other command line option
and is used only to list all available modules in dbitool. An example
execution is:

  module bsonread          raw   =>  row   (in memory)
  module bsonwrite         row   =>  raw   (in memory)
  module cassandraselect   raw   =>  row   
  module column            row   =>  row   
  module csvread           raw   =>  row   
  module csvwrite          row   =>  raw   
  module fileread          none  =>  raw   
  module filewrite         raw   =>  none  
  module fixedwidthread    raw   =>  row   
  module fixedwidthwrite   row   =>  raw   
  module gunzip            raw   =>  raw   
  module gzip              raw   =>  raw   
  module jsonread          raw   =>  row   (in memory)
  module jsonwrite         row   =>  raw   (in memory)
  module mysqlselect       raw   =>  row   
  module ndb               row   =>  row   
  module ndjsonread        raw   =>  row   
  module ndjsonwrite       row   =>  raw   
  module spreadsheetread   raw   =>  row   (in memory)
  module spreadsheetwrite  row   =>  none  (in memory)
  module sqliteinsert            =>        
  module sqliteselect      raw   =>  row   
  module stderr            raw   =>  none  
  module stdin             none  =>  raw   
  module stdout            raw   =>  none  
  module xmlread           raw   =>  row   (in memory)
  module xmlwrite          row   =>  raw   
  default row reads/writes per module iteration: 1024
  default error limit count: 3
  default row limit for in memory modules: 100000

All modules are listed by name with source and destination types (raw, row
or none). The "in memory" observation is used to indicate modules that have
to buffer rows in memory until EOF has been reached to process the data.

The last three lines shows the default values for stream size, error size
and memory limit options.

=head1 MODULE TREE

When parsing the commandline, dbitool builds a tree using streams to connect
the different node modules. If -v is used, the module tree is printed in the
log to help ilustrate how data is being directed between the different
modules. Here is an example of a module tree:

  module connections:
    fileread1 (output type raw) from file "test.csv"
      |-> csvread2 (output type row)
            |-> ndjsonwrite3 (output type raw)
                  |-> filewrite4 (output type none) to file "test.ndjson"
            |-> jsonwrite5 (output type raw)
                  |-> filewrite6 (output type none) to file "test.json"
                  |-> gzip7 (output type raw)
                        |-> filewrite8 (output type none) to file "test.json.gz"
                        |-> gunzip11 (output type raw)
                              |-> filewrite12 (output type none) to file "test2.json"
    log (output type row)
      |-> ndjsonwrite9 (output type raw)
            |-> filewrite10 (output type none) to file "a.log.ndjson"
      |-> column16 (output type row)
            |-> filewrite17 (output type none) to file "a.log"
      |-> logcsvwrite (output type raw)
            |-> logstdout (output type none)
    error (output type row)
      |-> errcsvwrite (output type raw)
            |-> errstderr (output type none)

There are three different root nodes: fileread1, log and error. Fileread1
module is reading "test.csv" file and sending it's data to csvread2 module,
and so on. The log module is sending it's data to ndjsonwrite9, column16 and
logcsvwrite modules. The same data goes to theese three modules. And finally
the error module is sending it's data to errcsvwrite.

After each module name (ex: fileread1, csvread2, etc) dbitool shows the
output data type for each module. Theese can be raw, row or none.

=head2 COLUMNS

All data is fed from one module to the next using columns. All data is
treated as tabular data. When reading from a file that has no column
information like a text file (ex: fileread module), the module will inform
that it's column names are: "col1".

Modules that read from tabular source, like databases or spreadsheets on
the other hand will inform the column names to their destination modules.
This information can/should be saved in the destination modules like
csvwrite, ndjsonwrite, etc.

=head2 DATA TYPES

The data type defines what kind of data each module reads or writes. Modules
can only be linked together if they have the same data type. A module that
ouputs rows can only be liked to modules that input rows, and so on.

=item

raw

Raw datatype is any data read form a file. Usually column name informed will
be "col1".

=item 

row

Row datatype is a row in a tabular data source using their original column
names. Each column data type is not checked or treaded in any way.

=item

none

Some modules have no input or output and in this case the data type is none.

=head1 MODULES

Specifying a module in the command line uses the following syntax: module
class and arguments. By using a shell, space and other shell sequences have
to be escaped:

  moduleclass:argument1=value1:argument2=value2:...

Usually several modules can be specified in order to read data from one
place, transform it and write it to somewhere else. Modules can also read
from the default log or error streams in order to save this data.

If dbitool is run with any level of verbose, the log will be output to the
standard output using stdout module. Therefore two modules cannot output to
stdout. If -v is used no other module can use stdout. It doesn't make sense
to mix log and data output anyway.

=head2 ARGUMENTS

=item

verbose=N

Set the verbosity level for this module only. If not used, the module will
use the same verbosity level of dbitool.

=item

in=INPUTSTREAM

Specity the input stream name from where the module will read it's data.
Not all modules require an input stream.

=item

out=OUTPUTSTREAM

Specity the output stream name to where the module will write it's data.
Not all modules require an output stream.

=head1 MODULE fileread

This module reads a file from disk (or other unix file descriptor) and
outputs data in a single column "col1". Data is read as text lines.

=head2 Arguments

=item

in=FILENAME

The in argument for fileread is not a stream but a filename to read from.

=head1 MODULE filewrite

This module writes one column from the input stream to a file.

=head2 Arguments

=item

out=FILENAME

The out argument for filewrite is not a stream but a filename to write to.

=head1 MODULE stdin

This module reads data from the dbitool's standard input and writes to a
stream. This module does not have an in argument.

=head2 Arguments

=item

out=STREAMNAME

The out argument for stdin module is the stream data is written to.

=head1 MODULE stdout

This module writes one column from the input stream to the dbitool's
standard output. If this module is used, verbose cannot be set. In this
case, to change the loglevel use --loglevel=N.

=head2 Arguments

=item

in=STREAMNAME

The in argument for stdout module is the stream data is read from.

=head1 STDIN AND STDOUT

Using dbitool's stdout module to output data and to save the log
information, one must set a stream to write the log file to a
different destination. The stdout module (or any other module) cannot
receive data from more than one source.

=head1 OMITTING STREAM NAMES

When using a single sequence of modules that read from the last one, it is
not necessary to name all streams. Dbitool will attribute the name "streamN"
to each stream used in sequence. For example: read a CSV (comma separated
values) file and write NDJSON (newline delimited JSON):

  dbitool fileread:in=a.csv:out=s1 csvread:in=s1:out=s2 \
    ndjsonwrite:in=s2:out=s3 filewrite:in=s3:out=b.ndjson

Is equivalent to:

  dbitool fileread:in=a.csv csvread ndjsonwrite filewrite:out=b.ndjson

Note that only the in argument for fileread and out argument for filewrite
are specified. The other stream names are implicit.

Stream name can also me ommited on the destination module, even if it's
explicit in the source module. This is ok:

  dbitool fileread:in=a.csv csvread:out=s1 ndjsonwrite filewrite:out=b.ndjson

=head1 FILENAME SHORTCUTS

It is not necessary to specify fileread or filewrite modules if only one
module will read or write from the corresponding streams. If the stream name
is preceded by "@", dbitool will instantiate a fileread (in case of an in
argument) or a filewrite (in case of an out argument) as a shortcut:

  dbitool fileread:in=a.csv csvread ndjsonwrite filewrite:out=b.ndjson

Is equivalent to:

  dbitool csvread:in=@a.csv ndjsonwrite:out=@b.ndjson

Filename shortcuts cannot be used if the file is read by more than one
module:

  dbitool csvread:in=@a.csv ndjsonwrite:out=@b.ndjson \
    bsonwrite:in=@a.csv:out=@c.bson

Instead, the fileread with the stream name has to be used:

  dbitool fileread:in=a.csv csvread:out=s1 ndjsonwrite:out=@b.ndjson \
    bsonwrite:in=s1:out=@c.bson

=head1 LOGGING

Dbitool writes all log information to a stream called log. The log stream
has three columns: time, mod and msg. Here is an example of log tuple:

  ("2018-06-27_09:59:27.943", "[main]", "dbitool start")

The parentesis and commas are perl syntax. There are three levels of
logging: 0 (none), 1 (normal), 2 (stats) and 3 (debug). Level 0 shows
nothing. Level 1 shows the module tree, start time and finished times. Level
2 adds column names and number of rows. Level 3 adds all data and byte
counts (very verbose, use with care!).

The log feed can't be save directly to a text file because it has three
columnns and a text file has only one. To save the log to a text file,
csvwrite can be used:

  dbitool csvread:in=@a.csv ndjsonwrite:out=@b.ndjson \
    csvwrite:in=log:sep=\ :header=0:out=dbitool.log

The sep argumento to csvwrite changes the separator from "," to espace and
the header=0 argument eliminates the CSV header on the log file.

The log stream can be saved to any kind of module: database, spreadsheet,
text file, CSV file, JSON file, BSON file, NDJSON file, etc. If dbitool is
executed in verbose mode (at least one -v), the log stream will be written
to STDOUT using csvwrite to concatenate all three columns into one text line.

=head1 ERROR HANDLING

When a module raises an exception, dbitool tries to continue normally. The
error is sent to the error module that can be saved thru another module. The
error stream has the same columns as the log stream.

  dbitool csvread:in=@a.csv ndjsonwrite:out=@b.ndjson \
    csvwrite:header=0:in=error:out=dbitool.err

To tap into the error feed, just read from a stream called "error". If there
is no error, the file will be empty because the header will not be written.

=head1 EXAMPLES

  perl -I. -MTestRow -e 'TestRow::csvfile(10)' | ./dbitool stdin csvread jsonwrite stdout | gzip

    Uses the TestRow testing class to generate a 10 row CSV file and convert
    it to JSON and output to stdout where it's compressed by gzip.

  dbitool -v -v fileread:in=test.csv csvread:header=1:out=s1 ndjsonwrite \
    filewrite:out=test.ndjson

    Reads test.csv, convert to NDJSON and write to test.ndjson. Log (stat)
    is show in stdout.

  cat /etc/motd | dbitool -v -v -v stdin jsonwrite filewrite:out=motd.json \
    column:clist=mod,msg:in=log filewrite:text=1:out=a.log

    Read the text file /etc/motd from STDIN, write it as a JSON file (one
    column) to motd.json and save module and message info from the log
    stream (debug) to a.log. (Log time is discarted)

=head1 CONSIDERATIONS

Dbitool can be used to create more than one main flow of data from different
independent sources into different destinations. There main advantages to
use dbitool this way is unify the log file, error file and reduce memory
footprint, as only one process will be allocated. But the drawback is
performance, because no threads are used and everything runs in one process,
if any module has to wait, it will impact performance of all data flows. If
all data access is local and there are no read locks, running one or
multiple processes could result in the same run time, but if there is any
wait time in data reads or writes the use of multiple dbitool process is
recommended.

Dbitool can gzip or gunzip data in the process tree but if compressing or
uncompressing are the first or final stage in the data flow, using external
gzip and/or gunzip is recommended for performance. In this case, use stdin
or stdout modules.

Memory usage can increase very much if using big data with in memory
modules. This is why there is a row count limit (--memorylimit) for reading
rows into memory.

=head1 SEE ALSO

DBI

=head1 AUTHOR

dbitool was written by Rodrigo Antunes: x@rora.com.br

=cut
