require 'zlib'

class Redis
  class HashRing

    POINTS_PER_SERVER = 160 # this is the default in libmemcached

    BUCKETS_NUMBER = 1024 # see: http://svn.php.net/viewvc/pecl/memcache/trunk/php_memcache.h?view=markup

    attr_reader :ring, :buckets, :sorted_keys, :replicas, :buckets_number, :nodes

    # nodes is a list of objects that have a proper to_s representation.
    # replicas indicates how many virtual points should be used pr. node,
    # replicas are required to improve the distribution.
    def initialize(nodes=[], replicas=POINTS_PER_SERVER, buckets_number=BUCKETS_NUMBER)
      @replicas = replicas
      @buckets_number = buckets_number
      @step = 0xffffffff / buckets_number
      @ring = {}
      @nodes = []
      @buckets = []
      @sorted_keys = []
      nodes.each do |node|
        add_node(node)
      end
      populate_buckets()
    end

    # Adds a `node` to the hash ring (including a number of replicas).
    def add_node(node)
      @nodes << node
      @replicas.times do |i|
        key = Zlib.crc32("#{node.id}-#{i}")
        @ring[key] = node
        @sorted_keys << key
      end
      @sorted_keys.sort!
      populate_buckets() if @buckets.size
    end

    def remove_node(node)
      @nodes.reject!{|n| n.id == node.id}
      @replicas.times do |i|
        key = Zlib.crc32("#{node.id}-#{i}")
        @ring.delete(key)
        @sorted_keys.reject! {|k| k == key}
      end
      populate_buckets() if @buckets.size
    end
    
    def populate_buckets
      @buckets_number.times do |i|
        @buckets[i] = hash_find(@step * i)
      end
    end

    def hash_find(point)
      lo = 0 
      mid = 0
      hi = @sorted_keys.size - 1

      loop do 
        #point is outside interval or lo >= hi, wrap-around
        if point <= @sorted_keys[lo] or point > @sorted_keys[hi]
          return @ring[@sorted_keys[lo]]
        end

        #test middle point
        mid = lo + (hi - lo) / 2

        #perfect match
        if point <= @sorted_keys[mid] and point > (mid ? @sorted_keys[mid-1] : 0)
          return @ring[@sorted_keys[mid]]
        end

        #too low, go up
        if point > @sorted_keys[mid]
          lo = mid + 1
        else
          hi = mid - 1
        end
      end
    end

    # get the node in the hash ring for this key
    def get_node(key)
      get_node_pos(key)[0]
    end

    def get_node_pos(key)
      return [nil,nil] if @ring.size == 0
      crc = Zlib.crc32(key)
      idx = crc % @buckets_number
      return [@buckets[idx], idx]
    end

    def iter_nodes(key)
      return [nil,nil] if @ring.size == 0
      node, pos = get_node_pos(key)
      @sorted_keys[pos..-1].each do |k|
        yield @ring[k]
      end
    end

  end
end
