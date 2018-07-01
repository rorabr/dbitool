package TestRow;

# TestRow - generate a procedurally created "random" row for testing - RORA - Montreal - 2018-06-28

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
use warnings;
use Data::Dumper;
use strict;

sub new {
  my($class)=shift;
  bless({
    word => ['cleanups','twinge','unkinger','studying','footers','jowars','whileen','laurus','recrates','michabou','aiglets','jokier','staghead','acronyx','kirsten','llareta','hothead','membrane','cheapens','unrolled','reactive','twattles','waddent','washtub','margent','anosmia','servings','sarcode','baloneys','onerate','untilt','outmoded','archduxe','cyclamin','watsonia','magellan','largando','triptyca','monogerm','gangster','favonius','araneous','estuous','jabots','magnetic','conjoint','nuthatch','santonic','filters','nomisms','tumulose','swiftest','custards','wingcut','cowgirls','riziform','zeoidei','coulters','vauntie','grappler','sexlike','sperms','boydekyn','agrised','maintain','apparens','rancidly','tartarum','naturism','kistfuls','dribblet','tangoed','divinity','fuzing','adermin','mocking','boorish','graded','porkchop','bequalm','offshore','decedent','wagwit','humblest','abietin','conusant','mangels','weighman','finary','choragic','voiced','oakwood','aconic','unsallow','sevres','realms','terence','unheaded','spooner','musicker','bedress','haircaps','cuculus','accouter','nalita','enteroid','momish','piccanin','rachitis','unvirtue','cafila','newline','stench','solera','spilled','grownups','ilongot','lecithin','egritude','adjoiner','spacial','bunters','moonlit','slidder','ethide','exostema','seamed','trickers','invade','reseed','verdun','quantal','kepped','schemas','silladar','antihuff','hederin','venulae','rocaille','bareboat','gunpoint','humiria','bumbarge','pemphix','rupert','aglaspis','puslike','saurian','muhlies','affiches','washery','reposal','digonous','aquilia','largish','medleys','belabor','yerbales','bimedial','bawbees','nances','welladay','cities','reflet','defeated','tickless','coynye','dogteeth','mellone','tiresome','amakebe','civitan','cabler','herling','vandal','lionize','pablum','hubbuboo','commixes','highroad','illumed','fulmarus','scorpius','ratify','scuttle','semiruin','crankle','moduli','jubilize','drawers','sandmen','stabbed','dumdums','tridii','homogeny','sogdoite','cabbagy','metiers','outlets','dukely','elemong','enthrill','devalues','chastity','khalifat','shiftily','patios','sweetest','chagoma','bathmat','dading','ricking','verdugo','tetrapla','lamboy','infernos','jazzes','dinette','ruglike','winging','unspread','unegal','unmeated','dhobee','shoulder','caladium','violater','velika','quetch','hornbeak','pinnate','docile','ragusye','inkweed','gemara','pungies','reattire','bludgeon','niceling','aleppo','clergy','koorhmn','deacon','figuring','druidic','gallnuts','girondin','unsourly','prasine','saladero','bingey','torpids','crinel','drawgate','cinerea','nubbly'],
    @_}, $class);
}

sub header {
  return("row","int32","float","date","hex","binary","uuencode","byte1","byte2","byte3","byte4","string");
}

# Generate a new row $n
sub row {
  my($self, $n)=@_;
  my $s = unpack("N", $self->{word}->[$n & 255]) ^ (0x82084211 * (($n+15) >> 1));
  my @b = unpack("C4", pack("N",$s));
  chomp(my $uu = pack("u", $s));
  $uu =~ s/,"'//gs;
  my @row = ($n, $s, sprintf("%f", unpack("f", pack("N", $s))), sprintf("%04d-%02d-%02dT%02d:%02d:%02d", 1990+$b[0]%40, $b[1]%12+1, $b[2]>>3+1, $b[2]%24, $b[3]>>3, $b[3]%60), sprintf("%x", $s), sprintf("%b", $b[3]) . sprintf("%b", $b[0]), $uu);
  my $name = "";
  map { push @row, ($b[$_]); $name .= ($name eq ""? "": " ") . $self->{word}->[$b[$_]]; } (0,1,2,3);
  push @row, ($name);
  return(@row);
}

# Check if row is correct
sub check {
  my($self, @row)=@_;
  my @cmp = $self->row($row[0]);
  die("incorrect number of columns (correct: " . $#cmp . ", row: " . $#row . ")") if ($#cmp != $#row);
  for (my $i = 0; $i <= $#cmp; $i++) {
    die("inconsistency in column $i (correct: \"" . $cmp[$i] . "\", row: \"" . $row[$i] . "\")") if ($cmp[$i] ne $row[$i]);
  }
}

# Generate a simple CSV file without quotes, call with:
# perl -I. -MTestRow -e 'TestRow::csv(3)'
sub csv {
  my($n)=@_;
  my $self = TestRow->new;
  print join(",", $self->header) . "\n";
  print join(",", $self->row($_)) . "\n" for (0 .. $n-1);
}

# Generate a "artificial" NDJSON file, call with:
# perl -I. -MTestRow -e 'TestRow::ndjson(3)'
sub ndjson {
  my($n, $sep)=@_;
  $sep = "\n" if (! defined $sep);
  my $self = TestRow->new;
  my @hdr = $self->header;
  for my $i (0 .. $n-1) {
    print "" . ($i > 0 ? $sep : "") . "{";
    my @row = $self->row($i);
    my $j = 0;
    for (@hdr) {
      print "" . ($j > 0 ? "," : "") . "\"$_\":\"$row[$j]\"";
      $j++;
    }
    print "}";
  }
}

# Generate a "artificial" JSON file, call with:
# perl -I. -MTestRow -e 'TestRow::json(3)'
sub json {
  my($n)=@_;
  my $self = TestRow->new;
  my @hdr = $self->header;
  print "[";
  &ndjson($n, ",");
  print "]";
}

# Class test function, call with:
# perl -I. -MTestRow -e 'TestRow::_test'
sub _test {
  my $self = TestRow->new;
  my @mem;
  for (my $i = 0; $i < 48; $i++) {
    my @row = $self->row($i);
    $mem[$i] = \@row;
    #print join(",", @row) . "\n";
  }
  $self->check(@{$_}) for (@mem);
}

1;

__END__

=head1 NAME

TestRow - generate a procedurally created "random" row for testing

=head1 SYNOPSIS

  use TestRow;

  # OO row generation
  $test = TestRow->new;
  $header = $test->header;
  $row = $test->row(1);
  $test->check($row);

  # file generation to stdout:
  TestRow::csv(3);
  TestRow::json(10);
  TestRow::ndjson(4);

=head1 DESCRIPTION

TestRow generates procedurally created pseudo random rows for testing. Each
row is a array ref that contains data of type: integer, float, binary, hex,
string, UU encode and a date. The method row returns a row based on the
parameter (as a seed). The function check raises an exception (die) in case
the row is inconsistent.

The function calls are to simplify the process of creating files for testing.
The single parameters is the number of rows and output is written to stdout.

=head1 AUTHOR

TestRow was written by Rodrigo Antunes. x@rora.com.br

=cut

