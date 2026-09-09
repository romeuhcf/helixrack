# frozen_string_literal: true

RSpec.describe HelixRack do
  it "has a version number" do
    expect(HelixRack::VERSION).not_to be nil
  end

  it "can call into Rust" do
    result = HelixRack.hello("world")

    expect(result).to eq("Hello world, from Rust!")
  end
end
