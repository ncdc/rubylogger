require 'atomic'

class DividerLFQ
  class Node
    attr_accessor :value, :next

    def initialize(value)
      @next = nil
      @value = value
    end
  end

  attr_reader :drops

  def initialize(max=nil)
    dummy = Node.new(nil)
    @first = dummy
    @divider = Atomic.new(dummy)
    @last = Atomic.new(dummy)
    @size = Atomic.new(0)
    @max = max
    @drops = 0
  end

  def push(value)
    if @max
      until @size.value <= @max
        pop
        @drops += 1
      end
    end

    current_last = @last.value
    new_node = Node.new(value)
    current_last.next = new_node
    @last.update {|_| new_node}
    @size.update {|s| s + 1}
    while @first != @divider.value do
      #puts 'cleanup'
      @first = @first.next
    end
  end

  def pop
    # get a point-in-time snapshot of the current divider node
    current_divider_node = @divider.value

    # if it's not currently pointing at the end of the queue
    # i.e. it's not empty
    if current_divider_node != @last.value
      next_node = current_divider_node.next
      result = next_node.value

      # it's possible that both a producer pushing and a consumer popping
      # could try to pop - in the event the queue is full, the producer
      # will pop until the size <= max
      #
      # try to update @divider to point to the next node, but it may
      # fail if the other thread has already moved it
      if @divider.compare_and_set(current_divider_node, next_node)
        # update succeeded, so we can decrement @size
        @size.update {|s| s - 1}
      else
        puts "unable to cas from #{current_divider_node} to #{next_node}"
      end
      return result
    end
    return nil
  end
end
