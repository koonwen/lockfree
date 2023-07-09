type 'a ptr = Null | Ptr of 'a ref

let ( !^ ) = function
  | Null -> invalid_arg "Attempt to dereference the null pointer"
  | Ptr r -> !r

let ( ^:= ) p v =
  match p with
  | Null -> invalid_arg "Attempt to assign the null pointer"
  | Ptr r -> r := v

type 'a elems = 'a option Atomic.t array

type 'a blk_lst = 'a blk ptr
and 'a blk = { elems : 'a elems; fields : 'a fields Atomic.t }

and 'a fields = { mark2 : bool; next : 'a blk ptr; mark1 : bool }
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
  add_dls : (int * 'a blk_lst) option Domain.DLS.key; (* add_head * add_block *)
  steal_dls : (int * 'a blk_lst * 'a blk_lst) option Domain.DLS.key;
      (* steal_head *     steal_prev    * steal_block  *)
}

let pp_print_elt pp_elt ppf atomic_elt_op =
  match Atomic.get atomic_elt_op with
  | None -> Format.pp_print_string ppf "None"
  | Some e -> pp_elt ppf e

let pp_print_elems pp_v ppf (elems : 'a elems) =
  let pp_v = pp_print_elt pp_v in
  let map_str =
    Array.map (fun elt -> Format.asprintf "%a" pp_v elt) elems |> Array.to_list
  in
  Format.fprintf ppf "[| %s |]" (String.concat "; " map_str)

let rec pp_print_blk_list pp_elt ppf = function
  | Null -> ()
  | Ptr v ->
      let { elems; fields } = !v in
      let fields = Atomic.get fields in
      pp_print_elems pp_elt ppf elems;
      Format.pp_print_string ppf " -> ";
      pp_print_blk_list pp_elt ppf fields.next

let pp_print_t pp_elt ppf { blk_lst_arr; _ } =
  let pp_v = pp_print_blk_list pp_elt in
  Array.iter (fun b -> Format.fprintf ppf "[] -> %a\n" pp_v b) blk_lst_arr

let print_int_bag t =
  Format.(fprintf std_formatter "@[%a@]@." (pp_print_t Format.pp_print_int) t)

let create ?(num_domains = Domain.recommended_domain_count ()) ?(blk_sz = 4096)
    () =
  assert (blk_sz > 0);
  {
    num_domains;
    blk_sz;
    blk_lst_arr = Array.make num_domains Null;
    add_dls = Domain.DLS.new_key (fun () -> None);
    steal_dls = Domain.DLS.new_key (fun () -> None);
  }

let add { add_dls; blk_sz; blk_lst_arr; _ } elem =
  let id = (Domain.self () :> int) in
  match Domain.DLS.get add_dls with
  | Some (head, blk) when head < blk_sz ->
      let { elems; _ } = !^blk in
      (* Set bit *)
      elems.(head) <- Atomic.make (Some elem);
      Domain.DLS.set add_dls (Some (head + 1, blk))
  | None (* Initialize *) | Some _ (* Rearched end of the array *) ->
      let elems = Array.make blk_sz (Atomic.make None) in
      elems.(0) <- Atomic.make (Some elem);
      (* WARNING THIS IS A PLACEHOLDER *)
      let fields =
        Atomic.make { mark2 = false; next = blk_lst_arr.(id); mark1 = false }
      in
      let new_blk_lst : 'a blk_lst = Ptr (ref { elems; fields }) in
      blk_lst_arr.(id) <- new_blk_lst;
      Domain.DLS.set add_dls (Some (1, new_blk_lst))

let steal { steal_dls; blk_sz; blk_lst_arr; num_domains; _ } =
  let rec loop id head blk_lst =
    match blk_lst with
    | Null (* Next linked list *) ->
        let next_id = (id + 1) mod num_domains in
        loop next_id 0 blk_lst_arr.(next_id)
    | Ptr blk ->
        let { elems; fields } = !blk in
        let { next; _ } = Atomic.get fields in
        if head >= blk_sz then loop id 0 next
        else
          let item = Atomic.get elems.(head) in
          if
            Option.is_some item && Atomic.compare_and_set elems.(head) item None
          then (
            (* Set new steal head *)
            Domain.DLS.set steal_dls (Some (head, Null, blk_lst));
            item)
          else loop id (head + 1) blk_lst
  in
  let id = (Domain.self () :> int) in
  match Domain.DLS.get steal_dls with
  | None (* Initialize *) -> loop id (-1) Null
  | Some (head, _prev_blk_lst, blk_lst) -> loop id head blk_lst

let try_remove_any ({ add_dls; blk_sz; _ } as t) =
  let rec loop head blk_lst =
    if head < 0 then
      match blk_lst with
      | Null (* Thread block list uninitialized *) -> steal t
      | Ptr blk ->
          let { fields; _ } = !blk in
          let { next; _ } = Atomic.get fields in
          if next = Null (* Last block *) then steal t
          else loop (blk_sz - 1) next
    else
      match blk_lst with
      | Null -> assert false
      | Ptr blk ->
          let { elems; _ } = !blk in
          let item = Atomic.get elems.(head) in
          if
            Option.is_some item && Atomic.compare_and_set elems.(head) item None
          then item
          else loop (head - 1) blk_lst
  in
  match Domain.DLS.get add_dls with
  | None (* Empty block list *) -> steal t
  | Some (head, blk_lst) -> loop (head - 1) blk_lst

(* let delete_blk ({steal_dls; _} : 'a t) =
   match Domain.DLS.get steal_dls with
   | None -> assert false
   | Some (head, prev_blk_lst, blk_lst) ->
     if prev_blk_lst != null && prev_blk_lst <> [] then
       match prev_blk_lst with
       | [] -> assert false
       | prev_blk :: next ->
         let m2, arr, m1 = !!prev_blk in
         if Atomic.compare_and_set prev_blk (m2, arr, m1) (true, next, m1) then
       )
*)
