#!/bin/env ruby

message_length = ARGV[0].to_i
message_rate = ARGV[1].to_f

i = 0
start = Time.now
interval = 1.0 / message_rate if message_rate > 0
loop do
  i += 1
  puts "#{i} #{'a' * message_length}"

  if message_rate > 0
    wait_till = start + (i * interval)
    delay = wait_till - Time.now
    if delay > 0
      sleep(delay)
    end
  end
end
