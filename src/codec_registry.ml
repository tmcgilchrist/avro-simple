(** Registry for compression codecs *)

module type CODEC = sig
  type t
  val name : string
  val create : unit -> t
  val compress : t -> bytes -> bytes
  val decompress : t -> bytes -> bytes
end

let registry : (string, (module CODEC)) Hashtbl.t = Hashtbl.create 10

let register name codec =
  Hashtbl.replace registry name codec  (* Replace if already exists *)

let get name =
  Hashtbl.find_opt registry name

let list () =
  Hashtbl.to_seq_keys registry |> List.of_seq
