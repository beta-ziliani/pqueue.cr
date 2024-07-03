require "atomic"

module PQueue
  # Priority queue implemented as a lock-free skiplist.
  #
  # The implementation is based on the accompanying code from the paper
  # "A Skiplist-Based Concurrent Priority Queue with Minimal Memory Contention"
  # by Jonatan Lindén and Bengt Jonsson
  class PQueue(K, V)
    # Maybe a struct?

    # The number of deletions before a restructure operation is attempted.
    property max_offset : Int32

    @head : Node(K, V)
    @tail : Node(K, V)

    # Number of levels in the skiplist.
    NUM_LEVELS = 32

    # Using the same name as in the origincal code: `is_marked_ref` means
    # that the node the pointer is pointing to has been logically deleted.
    private def is_marked_ref(ref : Pointer)
      (ref & 1).address == 1
    end

    # Mark a reference to a node as logically deleted.
    private def get_marked_ref(ref : Pointer)
      ref | 1
    end

    # Get the reference to a node without the logical delete flag set.
    private def get_unmarked_ref(ref : Pointer)
      ref & ~1
    end

    private class Node(K, V)
      getter k : K

      # The level of the node in the skiplist.
      property level : Int32

      # If the node is in the process of being inserted.
      property inserting : Bool = true

      property v : V

      def initialize(@k, @level, @v, @next : StaticArray(Pointer(Node(K, V)), NUM_LEVELS))
      end

      def initialize(@k, @level, @v)
        void = Pointer(Node(K, V)).null
        @next = StaticArray(Pointer(Node(K, V)), NUM_LEVELS).new void
      end
    end

    # *max_offset*: number of deletions before restructure is attempted
    def initialize(@max_offset)
      sentinel_k = uninitialized K
      sentinel_v = uninitialized V
      @tail = Node.new sentinel_k, NUM_LEVELS, sentinel_v
      p_tail = Pointer(Node(K, V)).new(@tail.object_id)
      tails = StaticArray(Pointer(Node(K, V)), NUM_LEVELS).new p_tail
      @head = Node.new sentinel_k, NUM_LEVELS, sentinel_v, tails
      @head.inserting = false
      @tail.inserting = false
    end

    private def cas(p : Pointer(Pointer(A)), expected : Pointer(A), new : Pointer(A)) : Bool forall A
      # TODO: See if we can use a more relaxed model
      Atomic::Ops.cmpxchg(p, expected, new, :sequentially_consistent, :sequentially_consistent)[1]
    end

    private def fetch_or(p : Pointer(Pointer(A)), value : UInt64) : Pointer(A) forall A
      {% unless flag?(:interpreted) %}
        # TODO: See if we can use a more relaxed model
        Pointer(A).new Atomic::Ops.atomicrmw(LLVM::AtomicRMWBinOp::Or, p.as(Pointer(UInt64)), value, LLVM::AtomicOrdering::SequentiallyConsistent, false)
      {% else %}
        # Not yet implemented, we need to emulate it
        old_p = p.value
        p.value = Pointer(A).new(p.value.address | value)
        old_p.as(Pointer(A))
      {% end %}
    end

    # Record predecessors and non-deleted successors of key k.  If k is
    # encountered during traversal of list, the node will be in succs[0].
    #
    # To detect skew in insert operation, return a pointer to the only
    # deleted node not having it's delete flag set.
    #
    # Skew example illustration, when locating 3. Level 1 is shifted in
    # relation to level 0, due to not noticing that s[1] is deleted until
    # level 0 is reached. (pointers in illustration are implicit, e.g.,
    # 0 --> 7 at level 2.)
    #
    #                   del
    #                   p[0]
    # p[2]  p[1]        s[1]  s[0]  s[2]
    #  |     |           |     |     |
    #  v     |           |     |     v
    #  _     v           v     |     _
    # | |    _           _	    v    | |
    # | |   | |    _    | |    _    | |
    # | |   | |   | |   | |   | |   | |
    #  0     1     2     4     6     7
    #  d     d     d
    private def locate_preds(k : K, preds : Slice(Pointer(Node(K, V))), succs : Slice(Pointer(Node(K, V)))) : Pointer(Node(K, V))
      d = false

      del = Pointer(Node(K, V)).null
      pred = Pointer(Node(K, V)).new @head.object_id
      i = NUM_LEVELS - 1

      while (i >= 0)
        cur = pred.as(Node(K, V)).@next[i]

        d = is_marked_ref cur
        cur = get_unmarked_ref cur

        # The original code requires sentinel nodes to have bottom and top elements.
        # Instead, we use `uninitialized`, meaning we can't acccess that field and need to guard it.
        while (c = cur.as(Node(K, V))) != @tail &&
              ((c.k < k || is_marked_ref(cur.as(Node(K, V)).@next[0])) || ((i == 0) && d))
          # Record bottom level deleted node not having delete flag
          # set, if traversed.
          del = cur if i == 0 && d

          pred = cur
          cur = pred.as(Node(K, V)).@next[i]
          d = is_marked_ref cur
          cur = get_unmarked_ref cur
        end

        preds[i] = pred
        succs[i] = cur
        i -= 1
      end
      del
    end

    # Insert a new node n with key k and value v.
    def insert(k : K, v : V) : Nil
      # The node will not be inserted if another node with key k is already
      # present in the list.
      # The predecessors, preds, and successors, succs, at all levels are
      # recorded, after which the node n is inserted from bottom to
      # top. Conditioned on that succs[i] is still the successor of
      # preds[i], n will be spliced in on level i.
      void = Pointer(Node(K, V)).null
      preds = StaticArray(Pointer(Node(K, V)), NUM_LEVELS).new void
      succs = StaticArray(Pointer(Node(K, V)), NUM_LEVELS).new void

      new = Node.new(k, rand(NUM_LEVELS), v)

      continue = true
      while continue
        # lowest level insertion retry loop
        del = locate_preds k, preds.to_slice, succs.to_slice

        # return if key already exists, i.e., is present in a non-deleted node
        if (n = succs[0].as(Node(K, V))) != @tail &&
           n.as(Node(K, V)).k == k &&
           !is_marked_ref(preds[0].as(Node(K, V)).@next[0]) &&
           preds[0].as(Node(K, V)).@next[0] == succs[0]
          new.inserting = false
          succs[0].as(Node(K, V)).v = v # update value
          return
        end

        new.@next[0] = succs[0]

        # The node is logically inserted once it is present at the bottom level.
        continue = !cas(preds[0].as(Node(K, V)).@next.to_unsafe, succs[0], Box.box(new).as(Pointer(Node(K, V))))
        # either succ has been deleted (modifying preds[0]),
        # or another insert has succeeded or preds[0] is head,
        # and a restructure operation has updated it
      end

      # Insert at each of the other levels in turn.
      i = 1
      while i < new.level
        # If successor of new is deleted, we're done. (We're done if
        # only new is deleted as well, but this we can't tell) If a
        # candidate successor at any level is deleted, we consider
        # the operation completed.
        break if is_marked_ref(new.@next[0]) || is_marked_ref(succs[i].as(Node(K, V)).@next[0]) || del == succs[i]

        # prepare next pointer of new node
        new.@next[i] = succs[i]

        if !cas(preds[i].as(Node(K, V)).@next.to_unsafe + i, succs[i], Box.box(new).as(Pointer(Node(K, V))))
          # failed due to competing insert or restructure
          del = locate_preds k, preds.to_slice, succs.to_slice
          # if new has been deleted, we're done
          break if succs[0] != new
        else
          # Succeeded at this level.
          i += 1
        end
      end

      # new is always something at this point (the commentted out if new)
      # this flag must be reset *after* all CAS have completed
      new.inserting = false # if new
    end

    # Update the head node's pointers from level 1 and up. Will locate
    # the last node at each level that has the delete flag set, and set
    # the head to point to the successor of that node. After completion,
    # if operating in isolation, for each level i, it holds that
    # head->next[i-1] is before or equal to head->next[i].
    #
    # Illustration valid state after completion:
    #
    #             h[0]  h[1]  h[2]
    #              |     |     |
    #              |     |     v
    #  _           |     v     _
    # | |    _     v     _	   | |
    # | |   | |    _    | |   | |
    # | |   | |   | |   | |   | |
    #  d     d
    private def restructure
      pred = Pointer(Node(K, V)).new(@head.object_id)
      i = NUM_LEVELS - 1

      while i > 0
        # the order of these reads must be maintained
        h = @head.@next[i] # record observed head

        {% unless flag?(:interpreted) %} Atomic.fence {% end %} # CMB() in the C code

        cur = pred.as(Node(K, V)).@next[i] # take one step forward from pred
        unless is_marked_ref(h.as(Node(K, V)).@next[0])
          i -= 1
          next
        end

        # traverse level until non-marked node is found
        # pred will always have its delete flag set
        while is_marked_ref(cur.as(Node(K, V)).@next[0])
          pred = cur
          cur = pred.as(Node(K, V)).@next[i]
        end

        # swing head pointer (in the paper, cur is pred.@next[i], but I think it's the same)
        i -= 1 if cas(@head.@next.to_unsafe + i, h, cur)
      end
    end

    # Delete and returns the element with smallest key in queue.
    def deletemin : {K, V}?
      # Try to update the head node's pointers, if offset > max_offset.
      #
      # Traverse level 0 next pointers until one is found that does
      # not have the delete bit set.
      x = Pointer(Node(K, V)).new(@head.object_id)
      offset = 0
      lvl = 0
      v = nil
      newhead = nil

      obs_head = x.as(Node(K, V)).@next[0]

      loop do
        # expensive, high probability that this cache line has
        # been modified
        nxt = x.as(Node(K, V)).@next[0]

        # tail cannot be deleted
        return nil if get_unmarked_ref(nxt).as(Node(K, V)) == @tail

        offset += 1

        # Do not allow head to point past a node currently being
        # inserted. This makes the lock-freedom quite a theoretic
        # matter.
        newhead = x if newhead.nil? && x.as(Node(K, V)).inserting

        if is_marked_ref(nxt)
          x = get_unmarked_ref nxt
          next
        end

        nxt = fetch_or(x.as(Node(K, V)).@next.to_unsafe, 1)
        x = get_unmarked_ref nxt

        break unless is_marked_ref(nxt)
      end

      v = {x.as(Node(K, V)).k, x.as(Node(K, V)).v}

      # if the offset is big enough, try to update the head node and
      # perform memory reclamation
      return v if offset <= @max_offset

      # Optimization. Marginally faster
      return v if @head.@next[0] != obs_head

      # If no inserting node was traversed, then use the latest
      # deleted node as the new lowest-level head pointed node
      # candidate.
      newhead = x if newhead.nil?

      # try to swing the lowest level head pointer to point to newhead,
      # which is deleted
      if cas(@head.@next.to_unsafe, obs_head, get_marked_ref(newhead))
        # Update higher level pointers.
        restructure

        # We successfully swung the upper head pointer. The nodes
        # between the observed head (obs_head) and the new bottom
        # level head pointed node (newhead) are guaranteed to be
        # non-live. Mark them for recycling.
        cur = get_unmarked_ref obs_head
        while cur != get_unmarked_ref newhead
          nxt = get_unmarked_ref cur.as(Node(K, V)).@next[0]
          cur = nxt
        end
      end

      v
    end

    # Return the elements in the queue as an array.
    def to_a : Array({K, V})
      a = [] of {K, V}
      x : Node(K, V) = @head

      loop do
        nxt = x.@next[0]

        nxt = get_unmarked_ref(nxt).as(Node(K, V))
        break if nxt == @tail

        a << {nxt.k, nxt.v} unless is_marked_ref(x.@next[0])
        x = nxt
      end
      a
    end

    def inspect(io : IO) : Nil
      x : Node(K, V) = @head

      io.puts "HEAD #{x.object_id.to_s(16)} [#{x.@next.map(&.address.to_s(16)).join(",")}]"
      loop do
        nxt = x.@next[0]

        nxt = get_unmarked_ref(nxt).as(Node(K, V))
        break if nxt == @tail

        deleted = is_marked_ref(x.@next[0]) ? "(d) " : ""
        io.puts "  #{nxt.k} #{nxt.v} #{deleted} [#{nxt.@next.map(&.address.to_s(16)).join(",")}]"
        x = nxt
      end

      io.puts "TAIL #{@tail.object_id.to_s(16)}"
    end
  end
end

# Additional functions required to perform operations on pointers
struct Pointer(T)
  def |(other : Int32) : Pointer(T)
    Pointer(T).new(address | other)
  end

  def &(other : Int32) : Pointer(T)
    Pointer(T).new(address & other)
  end
end
