require 'atomic'

class DanLFQ
  class Node
    attr_accessor :item

    def initialize(item, successor)
      @item = item
      @successor = Atomic.new(successor)
    end

    def successor
      @successor.value
    end

    def update_successor(old, new)
      @successor.compare_and_set(old, new)
    end
  end

  def initialize(max=nil)
    dummy_node = Node.new(:dummy, nil)

    @head = Atomic.new(dummy_node)
    @tail = Atomic.new(dummy_node)

    @size = Atomic.new(0)
    @drops = Atomic.new(0)
    @max = max
  end

  def push(item)
    if @max
      until @size.value <= @max
        pop
        @drops.update {|d| d + 1}
      end
    end

    # allocate a new node with the item embedded
    new_node = Node.new(item, nil)

    # keep trying until the operation succeeds
    loop do
      current_tail_node = @tail.value
      current_tail_successor = current_tail_node.successor

      # if our stored tail is still the current tail
      if current_tail_node == @tail.value
        # if that tail was really the last node
        if current_tail_successor.nil?
          # if we can update the previous successor of tail to point to this new node
          if current_tail_node.update_successor(nil, new_node)
            # then update tail to point to this node as well
            @tail.compare_and_set(current_tail_node, new_node)
            # and return
            @size.update {|s| s + 1}
            return true
            # else, start the loop over
          end
        else
          # in this case, the tail ref we had wasn't the real tail
          # so we try to set its successor as the real tail, then start the loop again
          @tail.compare_and_set(current_tail_node, current_tail_successor)
        end
      end
    end
  end

  def pop
    # retry until some value can be returned
    loop do
      # the value in @head is just a dummy node that always sits in that position,
      # the real 'head' is in its successor
      current_dummy_node = @head.value
      current_tail_node = @tail.value

      current_head_node = current_dummy_node.successor

      # if our local head is still consistent with the head node, continue
      # otherwise, start over
      if current_dummy_node == @head.value
        # if either the queue is empty, or falling behind
        if current_dummy_node == current_tail_node
          # if there's nothing after the 'dummy' head node
          if current_head_node.nil?
            # just return nil
            return nil
          else
            # here the head element succeeding head is not nil, but the head and tail are equal
            # so tail is falling behind, update it, then start over
            @tail.compare_and_set(current_tail_node, current_head_node)
          end
        # the queue isn't empty
        # if we can set the dummy head to the 'real' head, we're free to return the value in that real head, success
        elsif @head.compare_and_set(current_dummy_node, current_head_node)
          # grab the item from the popped node
          item = current_head_node.item

          if item != nil
            current_head_node.item = nil
          end

          # return it, success!
          @size.update {|s| s - 1}
          return item

        # else
          # try again
        end
      end
    end
  end

  def size; @size.value; end
  def drops; @drops.value; end
end
