          require 'pry'
module Chewy
  class Type
    module Import
      # This class purpose is to build ES client-acceptable bulk
      # request body from the passed objects for index and deletion.
      # It handles parent-child relationships as well by fetching
      # existing documents from ES, taking their `_parent` field and
      # using it in the bulk body.
      # If fields are passed - it creates partial update entries except for
      # the cases when the type has parent and parent_id has been changed.
      class BulkBuilder
        # @param type [Chewy::Type] desired type
        # @param index [Array<Object>] objects to index
        # @param delete [Array<Object>] objects or ids to delete
        # @param fields [Array<Symbol, String>] and array of fields for documents update
        def initialize(type, index: [], delete: [], fields: [])
          @type = type
          @index = index
          @delete = delete
          @fields = fields.map!(&:to_sym)
        end

        # Returns ES API-ready bulk requiest body.
        # @see https://github.com/elastic/elasticsearch-ruby/blob/master/elasticsearch-api/lib/elasticsearch/api/actions/bulk.rb
        # @return [Array<Hash>] bulk body
        def bulk_body
          @bulk_body ||= @index.flat_map(&method(:index_entry)).concat(
            @delete.flat_map(&method(:delete_entry))
          )
        end

        # The only purpose of this method is to cache document ids for
        # all the passed object for index to avoid ids recalculation.
        #
        # @return [Hash[String => Object]] an ids-objects index hash
        def index_objects_by_id
          @index_objects_by_id ||= index_object_ids.invert.stringify_keys!
        end

      private

        def crutches
          @crutches ||= Chewy::Type::Crutch::Crutches.new @type, @index
        end

        def parents
          return unless join_field

          @parents ||= begin
            ids = @index.map do |object|
              object.respond_to?(:id) ? object.id : object
            end
            ids.concat(@delete.map do |object|
              object.respond_to?(:id) ? object.id : object
            end)
            @type.filter(ids: {values: ids}).order('_doc').pluck(:_id, :_routing, join_field).map{|id, routing, join| [id, {routing: routing, parent_id: join['parent']}]}.to_h
          end
        end

        def find_parent(object)
          join = @type.compose(object, crutches)[join_field.to_s]
          if join
            join["parent"]
          else
            parents[object] if parents.present? #FIXME
          end
        end

        def existing_routing(object)
          #FIXME: UGLY AND SLOW!
          @type.filter(ids: {values: [object.id]}).pluck(:_routing).first
        end

        def routing(object)
          return unless object.respond_to?(:id) #non-model objects
          parent = find_parent(object)

          if parent
            #FIXME: UGLY AND SLOW!
            routing(indexed[parent]) || @type.filter(ids: {values: [parent]}).pluck(:_routing).first
          else
            object.id.to_s
          end
        end

        def indexed #FIXME: rename
          @indexed ||= @index.index_by(&:id)
        end

        #TODO move to a better place
        def join_field
          @join_field ||= @type.mappings_hash[@type.type_name.to_sym][:properties].find{|name, options| options[:type] == :join}&.first
        end

        def parent_changed?(data, old_parent)
          return false unless old_parent
          return false unless join_field
          return false unless @fields.include?(join_field)
          return false unless data.key?(join_field.to_s)

          # The join field value can be a hash, e.g.:
          # {"name": "child", "parent": "123"} for a child
          # {"name": "parent"} for a parent
          # but it can also be a string: (e.g. "parent") for a parent:
          # https://www.elastic.co/guide/en/elasticsearch/reference/current/parent-join.html#parent-join
          new_join_field_value = data[join_field.to_s]
          if new_join_field_value.is_a? Hash
            # If we have a hash in the join field,
            # we're taing the `parent` field that helds the parent id.
            new_parent_id = new_join_field_value["parent"]
            new_parent_id != old_parent[:parent_id]
          else
            # If there is a non-hash value (String or nil), it means that the join field is changed
            # and the current object is no longer a child.
            true
          end
        end

        def index_entry(object)
          entry = {}
          entry[:_id] = index_object_ids[object] if index_object_ids[object]

          data = @type.compose(object, crutches)
          if parents.present?
            parent = entry[:_id].present? && parents[entry[:_id].to_s]
          end

          entry[:_routing] = routing(object) if  routing(object) && join_field
          if parent_changed?(data, parent)
            entry[:data] = data
            delete = delete_entry(object).first
            index = {index: entry}
            [delete, index]
          elsif @fields.present?
            return [] unless entry[:_id]
            entry[:data] = {doc: @type.compose(object, crutches, fields: @fields)}
            [{update: entry}]
          else
            entry[:data] = data
            [{index: entry}]
          end
        end

        def delete_entry(object)
          entry = {}
          entry[:_id] = entry_id(object)
          entry[:_id] ||= object.as_json

          return [] if entry[:_id].blank?

          # load parents
          entry[:_routing] = existing_routing(object) if join_field
          if parents
            parent = entry[:_id].present? && parents[entry[:_id].to_s]
            if parent && parent[:parent_id]
              entry[:parent] = parent[:parent_id]
            end
          end

          [{delete: entry}]
        end

        def entry_id(object)
          if type_root.id
            type_root.compose_id(object)
          else
            id = object.id if object.respond_to?(:id)
            id ||= object[:id] || object['id'] if object.is_a?(Hash)
            id = id.to_s if defined?(BSON) && id.is_a?(BSON::ObjectId)
            id
          end
        end

        def index_object_ids
          @index_object_ids ||= @index.each_with_object({}) do |object, result|
            id = entry_id(object)
            result[object] = id if id.present?
          end
        end

        def type_root
          @type_root ||= @type.root
        end
      end
    end
  end
end
