require "./spec_helper"
require "wait_group"

describe PQueue::PQueue do
  it "inserts values" do
    pqueue = PQueue::PQueue(Int32, Int32).new 10
    max = 8000
    (1..max).each do |i|
      pqueue.insert(i, i)
    end

    a = pqueue.to_a
    a.size.should eq max
    (1..max).each do |i|
      a[i - 1].should eq({i, i})
    end
  end

  it "updates a value" do
    pqueue = PQueue::PQueue(Int32, Int32).new 10

    pqueue.insert(2, 2)
    pqueue.insert(1, 1)
    pqueue.insert(3, 3)

    pqueue.insert(2, 10)

    pqueue.to_a.should eq [{1, 1}, {2, 10}, {3, 3}]
  end

  it "is an enumerable" do
    pqueue = PQueue::PQueue(Int32, Int32).new 10

    pqueue.insert(2, 2)
    pqueue.insert(1, 1)
    pqueue.insert(3, 3)

    pqueue.map { |v| {v[0], v[1] + 1} }.should eq [{1, 2}, {2, 3}, {3, 4}]
  end

  it "deletes the min" do
    pqueue = PQueue::PQueue(Int32, Int32).new 10

    pqueue.insert(1, 1)
    pqueue.delete_min.should eq({1, 1})
    pqueue.to_a.should eq([] of {Int32, Int32})
  end

  it "deletes two times" do
    pqueue = PQueue::PQueue(Int32, Int32).new 10

    pqueue.insert(1, 1)
    pqueue.insert(2, 2)
    pqueue.delete_min.should eq({1, 1})
    pqueue.to_a.should eq([{2, 2}])
    pqueue.delete_min.should eq({2, 2})
    pqueue.to_a.should eq([] of {Int32, Int32})
  end

  it "performs multiple deletions" do
    pqueue = PQueue::PQueue(Int32, Int32).new 10

    (1..8000).each do |i|
      pqueue.insert(i, i)
    end

    (1..7200).each do |i|
      pqueue.delete_min.should eq({i, i})
    end

    a = pqueue.to_a
    a.size.should eq 8000 - 7200
    i = 7201
    j = 0
    while j < a.size
      a[j].should eq({i, i})
      j += 1
      i += 1
    end
  end

  it "performs parallel insertions" do
    pqueue = PQueue::PQueue(Int32, Int32).new 10

    wg = WaitGroup.new 8

    (0...8).each do |i|
      spawn do
        (1..1000).each do |j|
          k = i * 1000 + j
          pqueue.insert(k, k)
        end
        wg.done
      end
    end

    wg.wait

    a = pqueue.to_a
    i = 1
    j = 0
    while i < a.size
      el = a[j]?
      if el.nil?
        raise "#{j} >= #{a.size}"
      elsif el == {i, i}
        j += 1
      else
        raise "{#{i}, #{i}} dissappeared"
      end
      i += 1
    end
  end

  it "performs parallel deletions" do
    pqueue = PQueue::PQueue(Int32, Int32).new 10

    (1..8000).each do |i|
      pqueue.insert(i, i)
    end

    a = pqueue.to_a
    a.size.should eq 8000

    wg = WaitGroup.new 8

    (0...8).each do
      spawn do
        (1..900).each do
          v = pqueue.delete_min
          v.should_not be_nil
        end
        wg.done
      end
    end

    wg.wait

    # having 8000 - 8 * 900 = 800 elements left
    # they should be the upper ones
    a = pqueue.to_a
    a.size.should eq 800

    i = 8000 - 800 + 1
    j = 0
    while j < a.size
      a[j].should eq({i, i})
      j += 1
      i += 1
    end
  end

  it "performs parallel insertions and deletions" do
    pqueue = PQueue::PQueue(Int32, Int32).new 10

    fibers = 16
    delete_each = 100
    insert_each = 1000
    deleted = fibers // 2 * delete_each
    inserted = fibers // 2 * insert_each

    wg = WaitGroup.new fibers
    ch = Channel({Int32, Int32}?).new deleted

    (0...fibers).each do |i|
      if i % 2 == 0
        spawn do
          (1..insert_each).each do |j|
            k = (i//2) * insert_each + j
            pqueue.insert(k, k)
          end
          wg.done
        end
      else
        spawn do
          # wait a bit to let the other coroutines insert some elements
          sleep 1.millisecond
          (0...delete_each).each do
            t = pqueue.delete_min
            ch.send t
          end
          wg.done
        end
      end
    end

    wg.wait

    del = [] of {Int32, Int32}?
    (0...deleted).each do
      t = ch.receive
      del << t # if t
    end

    del.sort! do |a, b|
      a.nil? ? -1 : b.nil? ? 1 : a[0] <=> b[0]
    end
    a = pqueue.to_a

    a.sort.should eq a # check if the array is sorted
    del.size.should eq deleted
    a.size.should eq inserted - (del.reject &.nil?).size

    (1..inserted).each do |i|
      a.includes?({i, i}) || del.includes?({i, i}) || raise "{#{i}, #{i}} dissappeared"
    end
  end
end
