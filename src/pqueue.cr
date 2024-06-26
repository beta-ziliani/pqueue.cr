require "atomic"

module PQueue
  # or class?
  struct PQueue(K, V)
    property max_offset : Int32
    property head : Node(K, V)
    property tail : Node(K, V)

    NUM_LEVELS = 32

    class Node(K, V)
      getter k : K
      property level : Int32
      property inserting : Bool = true

      # next node is deleted
      property deleted : Bool = false
      property v : V

      def initialize(@k, @level, @v, @next : Array(Node(K, V)))
      end

      def initialize(@k, @level, @v)
        void = uninitialized Node(K, V)
        @next = Array(Node(K, V)).new NUM_LEVELS, void
      end

      def to_s(io)
        (0...@level).each do |n|
          if deleted
            io.puts "  #{k}[#{k} d] --> #{@next[n].k};"
          else
            io.puts "  #{k} --> #{@next[n].k};"
          end
        end
      end
    end

    # Init structure, setup sentinel head and tail nodes.
    # *max_offset*: number of deletions before restructure is attempted
    # *sentinel_min*: minimum key value
    # *sentinel_max*: maximum key value
    # *sentinel_v*: default sentinel value
    def initialize(@max_offset, sentinel_min, sentinel_max, sentinel_v)
      @tail = Node.new sentinel_max, NUM_LEVELS, sentinel_v
      tails = Array(Node(K, V)).new NUM_LEVELS, @tail
      @tail.@next.fill @tail
      @head = Node.new sentinel_min, NUM_LEVELS, sentinel_v, tails
      @head.inserting = false
      @tail.inserting = false
    end

    def cas(p : Pointer(A), expected : A, new : A) : Bool forall A
      Atomic::Ops.cmpxchg(p, expected, new, :sequentially_consistent, :sequentially_consistent)[1]
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
    def locate_preds(k : K, preds : Array(Node(K, V)), succs : Array(Node(K, V))) : Node(K, V)?
      d = false

      pred = @head
      i = NUM_LEVELS - 1
      while (i >= 0)
        cur = pred.@next[i]

        d = pred.deleted

        while (cur.k < k || cur.deleted) || ((i == 0) && d)
          # Record bottom level deleted node not having delete flag
          # set, if traversed.
          del = cur if (i == 0 && d)

          pred = cur
          cur = pred.@next[i]
          d = pred.deleted
        end
        preds[i] = pred
        succs[i] = cur
        i -= 1
      end
      del
    end

    # Insert a new node n with key k and value v.
    # The node will not be inserted if another node with key k is already
    # present in the list.
    # The predecessors, preds, and successors, succs, at all levels are
    # recorded, after which the node n is inserted from bottom to
    # top. Conditioned on that succs[i] is still the successor of
    # preds[i], n will be spliced in on level i.
    def insert(k : K, v : V) : Nil
      void = uninitialized Node(K, V)
      preds = Array(Node(K, V)).new NUM_LEVELS, void
      succs = Array(Node(K, V)).new NUM_LEVELS, void

      # critical_enter();

      new = Node.new(k, rand(32), v)

      continue = true
      while continue
        # lowest level insertion retry loop
        del = locate_preds k, preds, succs

        # return if key already exists, i.e., is present in a non-deleted node
        if (succs[0].k == k && !preds[0].@next[0].deleted && preds[0].@next[0] == succs[0])
          new.inserting = false
          succs[0].v = v # update value
          return
        end

        new.@next[0] = succs[0]

        # The node is logically inserted once it is present at the bottom level.
        continue = !cas(preds[0].@next.to_unsafe, succs[0], new)
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
        break if new.deleted || succs[i].deleted || del == succs[i]

        # prepare next pointer of new node
        new.@next[i] = succs[i]

        if !cas(preds[i].@next.to_unsafe + i, succs[i], new)
          # failed due to competing insert or restructure
          del = locate_preds k, preds, succs
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
    def restructure
      pred = @head
      i = NUM_LEVELS - 1

      while i > 0
        # the order of these reads must be maintained
        h = @head.@next[i] # record observed head

        # CMB() # TODO: memory barrier
        cur = pred.@next[i] # take one step forward from pred
        unless h.@next[0].deleted
          i -= 1
          next
        end

        # traverse level until non-marked node is found
        # pred will always have its delete flag set
        while cur.@next[0].deleted
          pred = cur
          cur = pred.@next[i]
        end

        # swing head pointer (in the paper, cur is pred.@next[i], but I think it's the same)
        i -= 1 if cas(@head.@next.to_unsafe + i, h, cur)
      end
    end

    # Delete element with smallest key in queue.
    # Try to update the head node's pointers, if offset > max_offset.
    #
    # Traverse level 0 next pointers until one is found that does
    # not have the delete bit set.
    def deletemin : {K, V}?
      x = @head
      offset = 0
      lvl = 0
      v = nil
      newhead = nil

      obs_head = x.@next[0]

      loop do
        offset += 1

        # expensive, high probability that this cache line has
        # been modified
        nxt = x.@next[0]

        # tail cannot be deleted
        return nil if nxt == @tail

        # Do not allow head to point past a node currently being
        # inserted. This makes the lock-freedom quite a theoretic
        # matter.
        newhead = x if newhead.nil? && x.inserting

        if x.deleted
          x = nxt
          next
        end

        # new_nxt = x.@next[0]
        x.deleted = true
        x = nxt

        break
      end

      v = {x.k, x.v}

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
      if cas(@head.@next.to_unsafe, obs_head, newhead)
        # Update higher level pointers.
        restructure

        # We successfully swung the upper head pointer. The nodes
        # between the observed head (obs_head) and the new bottom
        # level head pointed node (newhead) are guaranteed to be
        # non-live. Mark them for recycling.
        cur = obs_head
        while cur != newhead
          nxt = cur.@next[0]
          cur = nxt
        end
      end

      v
    end

    def to_s(io)
      io.puts "graph LR"

      x = @head
      while x != @tail
        x.to_s(io)
        x = x.@next[0]
      end
    end

    def to_a : Array({K, V})
      a = [] of {K, V}
      x = @head
      loop do
        nxt = x.@next[0]

        break if nxt == @tail

        a << {nxt.k, nxt.v} unless x.deleted
        x = nxt
      end
      a
    end
  end
end
