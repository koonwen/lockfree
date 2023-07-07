type 'a elt = 'a option Atomic.t
type 'a blk = (bool * 'a elt array * bool) Atomic.t

type 'a blk_lst = 'a blk list
(** mark1 = logical deletion of current block
    mark2 = next pointer from previous block
    E.g.
    [ 1;2;3;... ]  ->  [ 4;5;6;... ]  ->  [ 7;8;9;...]
                  mark2    mark1 
*)

type 'a t = {
  num_domains : int;
  blk_sz : int;
  blk_lst_arr : 'a blk_lst array;
  add_dls : (int * 'a blk_lst) option Domain.DLS.key;
  steal_dls : (int * 'a blk_lst) option Domain.DLS.key;
}

let ( !! ) = Atomic.get

let pp_print_elt pp_elt ppf atomic_elt_op =
  match Atomic.get atomic_elt_op with
  | None -> Format.fprintf ppf "None"
  | Some e -> Format.fprintf ppf "%a" pp_elt e

let pp_print_array pp_v ppf arr =
  let map_str =
    Array.map (fun elt -> Format.asprintf "%a" pp_v elt) arr |> Array.to_list
  in
  Format.fprintf ppf "[| %s |]" (String.concat "; " map_str)

let pp_print_block pp_elt ppf (blk : 'a blk) =
  let _m2, blk, _m1 = !!blk in
  let pp_v = pp_print_elt pp_elt in
  pp_print_array pp_v ppf blk

let pp_print_block_list pp_elt ppf (blk_lst : 'a blk_lst) =
  let pp_sep ppf () = Format.pp_print_string ppf " -> " in
  Format.(pp_print_list ~pp_sep (pp_print_block pp_elt) ppf blk_lst)

let pp_print_t pp_elt ppf { blk_lst_arr; _ } =
  let pp_print_blkls = pp_print_block_list pp_elt in
  Array.iter
    (fun b -> Format.fprintf ppf "[] -> %a\n" pp_print_blkls b)
    blk_lst_arr

let print_int_bag t =
  Format.(fprintf std_formatter "@[%a@]@." (pp_print_t Format.pp_print_int) t)

let create ?(num_domains = Domain.recommended_domain_count ()) ?(blk_sz = 4096)
    () =
  assert (blk_sz > 0);
  {
    num_domains;
    blk_sz;
    blk_lst_arr = Array.init num_domains (fun _ -> []);
    add_dls = Domain.DLS.new_key (fun () -> None);
    steal_dls = Domain.DLS.new_key (fun () -> None);
  }

let add { add_dls; blk_sz; blk_lst_arr; _ } elem =
  let id = (Domain.self () :> int) in
  match Domain.DLS.get add_dls with
  | Some (head, blk_lst) when head < blk_sz ->
      let _m2, blk, _m1 = !!(List.hd blk_lst) in
      (* Set bit *)
      blk.(head) <- Atomic.make (Some elem);
      Domain.DLS.set add_dls (Some (head + 1, blk_lst))
  | None (* Initialize *) | Some _ (* Rearched end of the array *) ->
      let new_blk = Array.make blk_sz (Atomic.make None) in
      new_blk.(0) <- Atomic.make (Some elem);
      let a_blk = Atomic.make (false, new_blk, false) in
      (* WARNING THIS IS A PLACEHOLDER *)
      let new_blk_lst = a_blk :: blk_lst_arr.(id) in
      blk_lst_arr.(id) <- new_blk_lst;
      Domain.DLS.set add_dls (Some (1, new_blk_lst))

let steal { steal_dls; blk_sz; blk_lst_arr; num_domains; _ } =
  let rec loop id head blk_lst =
    match blk_lst with
    | [] (* Next linked list *) ->
        let next_id = (id + 1) mod num_domains in
        loop next_id 0 blk_lst_arr.(next_id)
    | a_blk :: tl ->
        let _m2, blk, _m1 = !!a_blk in
        if head >= blk_sz then loop id 0 tl
        else
          let item = Atomic.get blk.(head) in
          if Option.is_some item && Atomic.compare_and_set blk.(head) item None
          then (
            (* Set new steal head *)
            Domain.DLS.set steal_dls (Some (head, blk_lst));
            item)
          else loop id (head + 1) blk_lst
  in
  let id = (Domain.self () :> int) in
  match Domain.DLS.get steal_dls with
  | None (* Initialize *) -> loop id (-1) []
  | Some (head, blk_lst) -> loop id head blk_lst

let try_remove_any ({ add_dls; blk_sz; _ } as t) =
  let rec loop head blk_lst =
    if head < 0 then
      match blk_lst with
      | [] (* Thread block list uninitialized *) | [ _ ] (* Last block *) ->
          steal t
      | _ :: tl -> loop (blk_sz - 1) tl
    else
      match blk_lst with
      | [] -> assert false
      | a_blk :: _ ->
          let _m2, blk, _m1 = !!a_blk in
          let item = Atomic.get blk.(head) in
          if Option.is_some item && Atomic.compare_and_set blk.(head) item None
          then item
          else loop (head - 1) blk_lst
  in
  match Domain.DLS.get add_dls with
  | None (* Empty block list *) -> steal t
  | Some (head, blk_lst) -> loop (head - 1) blk_lst
