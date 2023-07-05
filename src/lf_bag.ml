type 'a elt = 'a option Atomic.t
type 'a blk = 'a elt array
type 'a blks = 'a blk list

type 'a t = {
  num_domains : int;
  blk_sz : int;
  blks_arr : 'a blks array;
  add_dls : (int * 'a blks) option Domain.DLS.key;
  steal_dls : (int * 'a blks) option Domain.DLS.key;
}

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
  let pp_v = pp_print_elt pp_elt in
  pp_print_array pp_v ppf blk

let pp_print_block_list pp_elt ppf (blk_list : 'a blks) =
  let pp_sep ppf () = Format.pp_print_string ppf " -> " in
  Format.(pp_print_list ~pp_sep (pp_print_block pp_elt) ppf blk_list)

let pp_print_t pp_elt ppf { blks_arr; _ } =
  let pp_print_blkls = pp_print_block_list pp_elt in
  Array.iter
    (fun b -> Format.fprintf ppf "[] -> %a\n" pp_print_blkls b)
    blks_arr

let print_int_bag t =
  Format.(fprintf std_formatter "@[%a@]@." (pp_print_t Format.pp_print_int) t)

let create ?(num_domains = Domain.recommended_domain_count ()) ?(blk_sz = 4096)
    () =
  assert (blk_sz > 0);
  {
    num_domains;
    blk_sz;
    blks_arr = Array.init num_domains (fun _ -> []);
    add_dls = Domain.DLS.new_key (fun () -> None);
    steal_dls = Domain.DLS.new_key (fun () -> None);
  }

let add { add_dls; blk_sz; blks_arr; _ } elem =
  let id = (Domain.self () :> int) in
  match Domain.DLS.get add_dls with
  | Some (head, blk) when head < blk_sz ->
      let cur_blk = List.hd blk in
      (* Set bit *)
      cur_blk.(head) <- Atomic.make (Some elem);
      Domain.DLS.set add_dls (Some (head + 1, blk))
  | None (* Initialize *) | Some _ (* Rearched end of the array *) ->
      let new_blk = Array.make blk_sz (Atomic.make None) in
      new_blk.(0) <- Atomic.make (Some elem);
      let new_blk_lst = new_blk :: blks_arr.(id) in
      blks_arr.(id) <- new_blk_lst;
      Domain.DLS.set add_dls (Some (1, new_blk_lst))

let steal { steal_dls; blk_sz; blks_arr; num_domains; _ } =
  let rec loop id head blk_lst =
    match blk_lst with
    | [] (* Next linked list *) ->
        let next_id = (id + 1) mod num_domains in
        loop next_id 0 blks_arr.(next_id)
    | blk :: tl ->
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
      | blk :: _ ->
          let item = Atomic.get blk.(head) in
          if Option.is_some item && Atomic.compare_and_set blk.(head) item None
          then item
          else loop (head - 1) blk_lst
  in
  match Domain.DLS.get add_dls with
  | None (* Empty block list *) -> steal t
  | Some (head, blk_lst) -> loop (head - 1) blk_lst
