require "./spec_helper"
require "wait_group"

describe PQueue::PQueue do
  it "correctly performs insertions" do
    pqueue = PQueue::PQueue(Int32, Int32).new(10, 0, Int32::MAX, 0)
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

  it "correctly performs 1 deletion" do
    pqueue = PQueue::PQueue(Int32, Int32).new(10, 0, Int32::MAX, 0)

    pqueue.insert(1, 1)
    pqueue.deletemin.should eq({1, 1})
  end

  it "correctly performs 2 deletions" do
    pqueue = PQueue::PQueue(Int32, Int32).new(10, 0, Int32::MAX, 0)

    pqueue.insert(1, 1)
    pqueue.insert(2, 2)
    pqueue.deletemin.should eq({1, 1})
    pqueue.deletemin.should eq({2, 2})
  end

  it "correctly performs multiple deletions" do
    pqueue = PQueue::PQueue(Int32, Int32).new(0, 0, Int32::MAX, 0)

    (1..8000).each do |i|
      pqueue.insert(i, i)
    end

    (1..7200).each do |i|
      pqueue.deletemin.should eq({i, i})
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

  it "correctly performs parallel insertions" do
    pqueue = PQueue::PQueue(Int32, Int32).new(10, 0, Int32::MAX, 0)

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

  it "correctly performs parallel deletions" do
    pqueue = PQueue::PQueue(Int32, Int32).new(10, 0, Int32::MAX, 0)

    (1..8000).each do |i|
      pqueue.insert(i, i)
    end

    a = pqueue.to_a
    a.size.should eq 8000

    wg = WaitGroup.new 8

    (0...8).each do
      spawn do
        (1..900).each do
          v = pqueue.deletemin
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

  it "Parallel insertions and deletions" do
    pqueue = PQueue::PQueue(Int32, Int32).new(10, 0, Int32::MAX, 0)

    wg = WaitGroup.new 8
    ch = Channel({Int32, Int32}?).new 400

    (0...8).each do |i|
      if i % 2 == 0
        spawn do
          (1..1000).each do |j|
            k = (i//2) * 1000 + j
            pqueue.insert(k, k)
          end
          wg.done
        end
      else
        spawn do
          # wait a bit to let the other coroutines insert some elements
          sleep 1.millisecond
          (0...100).each do
            t = pqueue.deletemin
            ch.send t
          end
          wg.done
        end
      end
    end

    wg.wait

    del = [] of {Int32, Int32}
    (0...400).each do
      t = ch.receive
      del << t if t
    end

    del.sort!
    a = pqueue.to_a
    i = 1
    j = 0
    while i < a.size + del.size
      el = a[j]?
      if el.nil?
        raise "#{j} >= #{a.size}"
      elsif el == {i, i}
        j += 1
      else
        del.includes?({i, i}) || raise "{#{i}, #{i}} dissappeared"
      end
      i += 1
    end
  end
end
