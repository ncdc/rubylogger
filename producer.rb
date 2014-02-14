#!/bin/env ruby

message_length = ARGV[0].to_i
delay = ARGV[1].to_f

i = 0
loop do
  i += 1
  puts "#{i} #{'a' * message_length}"
  sleep delay
end
