# pqueue.cr

Crystal implementation of [Lindén and Jonsson's lock-free priority queue](https://github.com/jonatanlinden/PR).

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     pqueue:
       github: beta-ziliani/pqueue.cr
   ```

2. Run `shards install`

## Usage

```crystal
require "pqueue"

# The queue retains a number of deleted nodes before restructuring the internal structure.
# The number in the initializer is the count of such nodes.
queue = PQueue::PQueue(Int32, String).new 10

# Insert some elements
queue.insert 10, "low prio"
queue.insert 5, "mid prio"

# It also works with the indexing operator
queue[0] = "high priority"

# Inserting also updates if the key exists
queue.insert 5, "mid priority"

# Which of course can be done with the indexing operator too
queue[10] = "low priority"

a = [] of {Int32, String}?

(1..4).each do
  a << queue.delete_min
end

puts a # => [{0, "high priority}, {5, "mid priority"}, {10, "low priority"}, nil]
```

## Contributing

1. Fork it (<https://github.com/your-github-user/pqueue/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Beta Ziliani](https://github.com/beta-ziliani) - creator and maintainer
