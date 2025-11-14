(** Logical type conversion helpers.

    This module provides type-safe codecs for Avro logical types that integrate
    with popular OCaml libraries like Ptime, Uuidm, and Zarith.

    Logical types extend Avro's type system with higher-level semantics while
    maintaining wire compatibility with the underlying primitive types.

    {1 Example Usage}

    {[
      open Avro
      open Avro_logical

      module Order = struct
        type t = {
          id: Uuidm.t;
          created_at: Ptime.t;
          total: Z.t;  (* amount in cents *)
        }

        let codec =
          Codec.record
          |> Codec.field "id" uuid (fun o -> o.id)
          |> Codec.field "created_at" timestamp_micros (fun o -> o.created_at)
          |> Codec.field "total" (decimal ~precision:10 ~scale:2) (fun o -> o.total)
          |> Codec.seal
      end
    ]}
*)

(** {1 Temporal Types}

    Temporal logical types represent dates and times with various precisions.
    All types use Ptime for type-safe date/time handling.
*)

val date : (int * int * int) Codec.t
(** [date] encodes dates as days since Unix epoch (1970-01-01).

    Dates are represented as [(year, month, day)] tuples where:
    - year is the full year (e.g., 2024)
    - month is 1-12
    - day is 1-31

    Schema: [\{type: "int", logicalType: "date"\}]

    Example:
    {[
      let today = (2024, 11, 13) in
      let encoded = Codec.encode date today
    ]}

    @raise Invalid_argument if date is invalid *)

val time_millis : (int * int * int * int) Codec.t
(** [time_millis] encodes time as milliseconds since midnight.

    Time is represented as [(hour, minute, second, millisecond)] tuples where:
    - hour is 0-23
    - minute is 0-59
    - second is 0-59
    - millisecond is 0-999

    Schema: [\{type: "int", logicalType: "time-millis"\}]

    Valid range: 0 to 86400000 milliseconds (exclusive)

    @raise Invalid_argument if time is invalid *)

val time_micros : (int * int * int * int) Codec.t
(** [time_micros] encodes time as microseconds since midnight.

    Time is represented as [(hour, minute, second, microsecond)] tuples where:
    - hour is 0-23
    - minute is 0-59
    - second is 0-59
    - microsecond is 0-999999

    Schema: [\{type: "long", logicalType: "time-micros"\}]

    Valid range: 0 to 86400000000 microseconds (exclusive)

    @raise Invalid_argument if time is invalid *)

val timestamp_millis : int64 Codec.t
(** [timestamp_millis] encodes UTC timestamp as milliseconds since Unix epoch.

    Timestamps are represented as [int64] milliseconds since 1970-01-01T00:00:00Z.

    Schema: [\{type: "long", logicalType: "timestamp-millis"\}]

    Example:
    {[
      let now_ms = Int64.of_float (Unix.time () *. 1000.) in
      let encoded = Codec.encode timestamp_millis now_ms
    ]} *)

val timestamp_micros : int64 Codec.t
(** [timestamp_micros] encodes UTC timestamp as microseconds since Unix epoch.

    Timestamps are represented as [int64] microseconds since 1970-01-01T00:00:00Z.

    Schema: [\{type: "long", logicalType: "timestamp-micros"\}]

    Example:
    {[
      let now_us = Int64.of_float (Unix.time () *. 1_000_000.) in
      let encoded = Codec.encode timestamp_micros now_us
    ]} *)

val local_timestamp_millis : int64 Codec.t
(** [local_timestamp_millis] encodes local timestamp (no timezone) as milliseconds.

    Like [timestamp_millis] but represents a "local" datetime without timezone info.
    The actual timezone is not stored and must be inferred from context.

    Schema: [\{type: "long", logicalType: "local-timestamp-millis"\}] *)

val local_timestamp_micros : int64 Codec.t
(** [local_timestamp_micros] encodes local timestamp (no timezone) as microseconds.

    Like [timestamp_micros] but represents a "local" datetime without timezone info.

    Schema: [\{type: "long", logicalType: "local-timestamp-micros"\}] *)

(** {1 Numeric Types}

    Numeric logical types support arbitrary-precision decimal numbers using Zarith.
*)

val decimal : precision:int -> scale:int -> Z.t Codec.t
(** [decimal ~precision ~scale] encodes arbitrary-precision decimal numbers.

    Uses Zarith's [Z.t] for big integers. The unscaled value is stored as
    variable-length bytes in big-endian two's-complement format.

    - [precision] is the maximum number of digits (must be positive)
    - [scale] is the number of digits to the right of the decimal point (must be non-negative)

    The value is stored as: [unscaled_value = decimal_value × 10^scale]

    Schema: [\{type: "bytes", logicalType: "decimal", precision: P, scale: S\}]

    Example:
    {[
      (* Store 99.99 with precision=10, scale=2 *)
      let price = Z.of_int 9999 in  (* 99.99 * 10^2 *)
      let price_codec = decimal ~precision:10 ~scale:2 in
      let encoded = Codec.encode price_codec price
    ]}

    @raise Invalid_argument if precision <= 0 or scale < 0 *)

val decimal_fixed : precision:int -> scale:int -> size:int -> Z.t Codec.t
(** [decimal_fixed ~precision ~scale ~size] encodes decimal as fixed-size bytes.

    Like [decimal] but uses a fixed-size byte array instead of variable-length bytes.
    Useful when the maximum size is known and you want consistent record sizes.

    - [size] is the fixed number of bytes (must be sufficient for precision)

    Schema: [\{type: "fixed", size: N, name: "decimal_N", logicalType: "decimal",
              precision: P, scale: S\}]

    @raise Invalid_argument if size is too small for the precision *)

(** {1 String Types}

    String-based logical types for structured string data.
*)

val uuid : Uuidm.t Codec.t
(** [uuid] encodes UUIDs as strings.

    Uses the Uuidm library for type-safe UUID handling. UUIDs are encoded
    as canonical lowercase hex strings with hyphens (36 characters).

    Schema: [\{type: "string", logicalType: "uuid"\}]

    Example:
    {[
      let id = Uuidm.v4_gen (Random.State.make_self_init ()) () in
      let encoded = Codec.encode uuid id
    ]} *)

(** {1 Duration}

    Duration represents an amount of time with three separate components.
*)

type duration = {
  months: int;  (** Number of months *)
  days: int;    (** Number of days *)
  millis: int;  (** Number of milliseconds *)
}
(** Duration represents a period of time with three independent components.

    Note: This is not anchored to any specific point in time, so the actual
    elapsed time depends on the calendar. For example, "1 month" varies in
    length depending on which month.

    All fields are stored as unsigned 32-bit integers in little-endian format. *)

val duration : duration Codec.t
(** [duration] encodes durations as a 12-byte fixed record.

    The duration is encoded as three consecutive 4-byte little-endian unsigned integers:
    - Bytes 0-3: months
    - Bytes 4-7: days
    - Bytes 8-11: milliseconds

    Schema: [\{type: "fixed", size: 12, logicalType: "duration"\}]

    Example:
    {[
      let period = { months = 6; days = 15; millis = 5000 } in
      let encoded = Codec.encode duration period
    ]}

    @raise Invalid_argument if any component is negative *)

(** {1 Custom Conversions}

    Create your own logical type conversions.
*)

val make_logical :
  name:string ->
  base_codec:'a Codec.t ->
  encode:('b -> 'a) ->
  decode:('a -> 'b) ->
  'b Codec.t
(** [make_logical ~name ~base_codec ~encode ~decode] creates a custom
    logical type converter.

    This allows you to define custom logical types that aren't in the Avro
    specification but follow the same pattern.

    - [name] is the logical type name (for documentation/debugging)
    - [base_codec] is the underlying Avro codec
    - [encode] converts from your high-level type to the base type
    - [decode] converts from the base type back to your high-level type

    Example:
    {[
      (* Custom "money-cents" type that stores dollars as integer cents *)
      type money = { dollars: int; cents: int }

      let money_to_cents m = m.dollars * 100 + m.cents
      let cents_to_money c =
        { dollars = c / 100; cents = c mod 100 }

      let money_codec =
        make_logical
          ~name:"money-cents"
          ~base_codec:Codec.int
          ~encode:money_to_cents
          ~decode:cents_to_money
    ]} *)
