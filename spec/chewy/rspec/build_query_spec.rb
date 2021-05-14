require 'spec_helper'

describe :build_query do
  before do
    stub_model(:city)
    stub_index(:cities) { index_scope City }
    CitiesIndex.create
  end

  let(:dummy_query) { {} }
  let(:expected_query) { {index: ['cities'], body: {}} }
  let(:unexpected_query) { {} }

  context 'build expected query' do
    specify do
      expect(CitiesIndex.query(dummy_query)).to build_query(expected_query)
    end
  end

  context 'not to build unexpected query' do
    specify do
      expect(CitiesIndex.query(dummy_query)).not_to build_query(unexpected_query)
    end
  end
end
