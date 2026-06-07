resource "aws_kinesis_stream" "new_order" {
  name        = "dijkfood-new-order"
  shard_count = 1
}

resource "aws_kinesis_stream" "order_events" {
  name        = "dijkfood-order-events"
  shard_count = 1
}

resource "aws_kinesis_stream" "courier_positions" {
  name        = "dijkfood-courier-positions"
  shard_count = 1
}

resource "aws_kinesis_stream" "allocation_events" {
  name        = "dijkfood-allocation-events"
  shard_count = 1
}
