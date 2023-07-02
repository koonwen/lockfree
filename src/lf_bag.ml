[@@@warning "-32"]
(* module S : sig *)
(*   type 'a elt = 'a option Atomic.t *)
(*   type 'a block = 'a elt array *)

(*   type 'a t = { *)
(*     num_domains : int; *)
(*     blk_sz : int; *)
(*     blocks : 'a block list array; *)
(*     add_head : (int * 'a block list) option Domain.DLS.key; *)
(*     steal_head : (int * 'a block list) option Domain.DLS.key; *)
(*   } *)

(*   val create : ?num_domains:int -> ?blk_sz:int -> unit -> 'a t *)
(*   val add : 'a t -> 'a -> unit *)
(*   val try_remove : 'a t -> 'a option *)
(* end *)

module S = struct
  type 'a elt = 'a option Atomic.t
  type 'a block = 'a elt array

  type 'a t = {
    num_domains : int;
    blk_sz : int;
    blocks : 'a block list array;
    add_head : (int * 'a block list) option Domain.DLS.key;
    steal_head : (int * 'a block list) option Domain.DLS.key;
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

  let pp_print_block pp_elt ppf (blk : 'a block) =
    let pp_v = pp_print_elt pp_elt in
    pp_print_array pp_v ppf blk

  let pp_print_block_list pp_elt ppf (blk_list : 'a block list) =
    let pp_sep ppf () = Format.pp_print_string ppf " -> " in
    Format.(pp_print_list ~pp_sep (pp_print_block pp_elt) ppf blk_list)

  let pp_print_t pp_elt ppf { blocks; _ } =
    let pp_print_blkls = pp_print_block_list pp_elt in
    Array.iter
      (fun b -> Format.fprintf ppf "[] -> %a\n" pp_print_blkls b)
      blocks

  let print_int_bag t =
    Format.(fprintf std_formatter "@[%a@]@." (pp_print_t Format.pp_print_int) t)

  let create ?(num_domains = Domain.recommended_domain_count ())
      ?(blk_sz = 4096) () =
    {
      num_domains;
      blk_sz;
      blocks = Array.init num_domains (fun _ -> []);
      add_head = Domain.DLS.new_key (fun () -> None);
      steal_head = Domain.DLS.new_key (fun () -> None);
    }

  let add { add_head; blk_sz; blocks; _ } elem =
    let id = (Domain.self () :> int) in
    match Domain.DLS.get add_head with
    | Some (head, blk) when head < blk_sz ->
        let cur_blk = List.hd blk in
        cur_blk.(head) <- Atomic.make (Some elem);
        Domain.DLS.set add_head (Some (head + 1, blk))
    | None (* Initialize *) | Some _ (* End of the array *) ->
        let new_blk = Array.make blk_sz (Atomic.make None) in
        new_blk.(0) <- Atomic.make (Some elem);
        let new_blk_lst = new_blk :: blocks.(id) in
        blocks.(id) <- new_blk_lst;
        Domain.DLS.set add_head (Some (1, new_blk_lst))

  let rec steal ({ steal_head; blk_sz; blocks; num_domains; _ } as t) id head =
    function
    | [] (* Next linked list *) ->
        let next_id = (id + 1) mod num_domains in
        steal t next_id (blk_sz - 1) blocks.(next_id)
    | blk :: tl as blk_lst ->
        if head >= blk_sz then steal t id 0 tl
        else
          let item = Atomic.get blk.(head) in
          if Option.is_some item && Atomic.compare_and_set blk.(head) item None
          then (
            Domain.DLS.set steal_head (Some (head, blk_lst));
            item)
          else steal t id (head + 1) blk_lst

  let rec try_remove_any ({ add_head; steal_head; blk_sz; blocks; _ } as t) =
    let id = (Domain.self () :> int) in
    let rec loop head blk_lst =
      if head < 0 then
        match blk_lst with
        | [] | [ _ ] -> steal t id head []
        | _ :: tl -> loop (blk_sz - 1) tl
      else
        match blk_lst with
        | [] -> assert false
        | blk :: _ ->
            let item = Atomic.get blk.(head) in
            if
              Option.is_some item && Atomic.compare_and_set blk.(head) item None
            then (
              Domain.DLS.set steal_head (Some (head, blk_lst));
              item)
            else loop (head - 1) blk_lst
    in
    match Domain.DLS.get steal_head with
    | Some (head, blk_lst) -> loop head blk_lst
    | None (* Initialize steal_head *) -> (
        match Domain.DLS.get add_head with
        | None (* Empty block list *) ->
            Domain.DLS.set steal_head (Some (-1, blocks.(id)));
            try_remove_any t
        | Some (head, blk_lst) ->
            Domain.DLS.set steal_head (Some (head - 1, blk_lst));
            loop (head - 1) blk_lst)
end
