#!/bin/env ruby

require './divider_lfq'
require './dan_lfq'
require 'socket'
require 'objspace'
require 'pp'

class Reader
  attr_reader :max_read_len, :stats
  NEWLINE = "\n"

  def initialize(args)
    @queue = args[:queue]
    @max_read_len = args[:max_read_len] || 2048
    @io = args[:io]

    @stats = {
      total: 0, # total messages read
      time: 0   # cumulative micros spent between reads
    }
  end

  def read
    loop do
      line = @io.gets(NEWLINE, @max_read_len)

      start = Time.now

      unless line
        @queue.push(:done)
        break
      end

      @queue.push(line)

      @stats[:total] += 1
      @stats[:time] += ((Time.now - start) * 1000000).to_i
    end

    @stats[:avg_time_per_event] = (@stats[:time].to_f / @stats[:total].to_f).round(2)
  end
end

class SyslogWriter
  attr_reader :stats
  NEWLINE = "\n"

  def initialize(args)
    @queue = args[:queue]
    @empty_wait_period = args[:empty_wait_period] || 0.1
    @socket_path = args[:socket_path] || '/dev/log'

    @stats = {
      total: 0,     # total processed count
      drops: 0,     # total dropped
      partials: 0,  # total partial writes
      delivered: 0, # total delivered
      time: 0       # cumulative micros spent writing
    }

    @facility_code = 1
    @severity_code = 6
    @priority = (@facility_code * 8) + @severity_code
    @source_host = "host"
    @app_name = "app"
    @proc_id = "1"
    @msg_id = "1"
  end

  def write
    @socket ||= Addrinfo.unix(@socket_path, :DGRAM).connect

    loop do
      line = @queue.pop

      start = Time.now

      if line.nil?
        sleep @empty_wait_period
        next
      elsif line == :done
        break
      end

      @stats[:total] += 1

      timestamp = start.strftime("%Y-%m-%dT%H:%M:%S%z")
      syslog_msg = "<#{@priority}>1 #{timestamp} #{@source_host} #{@app_name} #{@proc_id} #{@msg_id} - #{line.strip}\n"

      begin
        written = @socket.write_nonblock(syslog_msg)
        if written != syslog_msg.length
          @stats[:partials] += 1
        else
          @stats[:delivered] += 1
        end
      rescue => e
        @stats[:drops] += 1
      end

      @stats[:time] += ((Time.now - start) * 1000000).to_i
    end

    compute_stats
  end

  private
  def compute_stats
    [:delivered, :drops, :partials].each do |stat|
      @stats["#{stat}_pct".to_sym] = ((@stats[stat].to_f / @stats[:total].to_f) * 100).round(1)
    end

    @stats[:avg_time_per_event] = (@stats[:time].to_f / @stats[:total].to_f).round(2)
  end
end

class LogManager
  attr_reader :stats

  def initialize(args)
    @reader = args[:reader]
    @writer = args[:writer]

    @stats = {
      time: 0 # seconds
    }
  end

  def run
    start = Time.now

    writer_thread = Thread.new do
      @writer.write
    end

    reader_thread = Thread.new do
      @reader.read
    end

    reader_thread.join
    writer_thread.join

    @stats[:time] = (Time.now - start).round(3)
  end
end

max_queue_size = ARGV[0].to_i

#queue = DanLFQ.new(@max_queue_size)
queue = DividerLFQ.new(@max_queue_size)

writer = SyslogWriter.new(queue: queue, empty_wait_period: 0.1, socket_path: '/dev/log')
reader = Reader.new(queue: queue, max_read_len: 2048, io: $stdin)
logman = LogManager.new(reader: reader, writer: writer)

logman.run

stats = {
  reader: reader.stats,
  writer: writer.stats,
  queue_drops: queue.drops
}

puts stats

#GC.start
pp GC.stat
