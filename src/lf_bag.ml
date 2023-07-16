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
and 'a blk = { elems : 'a elems; a_fields : 'a fields Atomic.t }

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
  notif_arr : bool Atomic.t array;
  add_dls : (int * 'a blk_lst) option Domain.DLS.key; (* add_head * add_block *)
  steal_dls : (int * 'a blk_lst * 'a blk_lst) option Domain.DLS.key;
      (* steal_head * steal_prev * steal_block  *)
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
  | Null -> Format.pp_print_string ppf "Null"
  | Ptr v ->
      let { elems; a_fields } = !v in
      let fields = Atomic.get a_fields in
      pp_print_elems pp_elt ppf elems;
      Format.pp_print_string ppf " -> ";
      pp_print_blk_list pp_elt ppf fields.next

let pp_print_t pp_elt ppf { blk_lst_arr; _ } =
  let pp_v = pp_print_blk_list pp_elt in
  Array.iter (fun b -> Format.fprintf ppf "[] -> %a\n" pp_v b) blk_lst_arr

let print_int_bag t =
  Format.(
    fprintf std_formatter "@[BAG(%a)@]@." (pp_print_t Format.pp_print_int) t)

let create ?(num_domains = Domain.recommended_domain_count ()) ?(blk_sz = 4096)
    () =
  assert (blk_sz > 0);
  {
    num_domains;
    blk_sz;
    blk_lst_arr = Array.make num_domains Null;
    notif_arr = Array.init num_domains (fun _ -> Atomic.make false);
    add_dls = Domain.DLS.new_key (fun () -> None);
    steal_dls = Domain.DLS.new_key (fun () -> None);
  }

let add { add_dls; blk_sz; blk_lst_arr; notif_arr; _ } elem =
  Printf.printf "Trying to add\n";
  let id = (Domain.self () :> int) in
  match Domain.DLS.get add_dls with
  | Some (head, blk) when head < blk_sz ->
      let { elems; _ } = !^blk in
      (* Set bit *)
      Printf.printf "Adding %d\n%!" elem;
      Atomic.set notif_arr.(id) false;
      Atomic.set elems.(head) (Some elem);
      Domain.DLS.set add_dls (Some (head + 1, blk))
  | None (* Initialize *) | Some _ (* Rearched end of the array *) ->
      Printf.printf "Adding new blk\n";
      let elems = Array.init blk_sz (fun _ -> Atomic.make None) in
      Atomic.set elems.(0) (Some elem);
      (* WARNING THIS IS A PLACEHOLDER *)
      let a_fields =
        Atomic.make { mark2 = false; next = blk_lst_arr.(id); mark1 = false }
      in
      let new_blk_lst : 'a blk_lst = Ptr (ref { elems; a_fields }) in
      blk_lst_arr.(id) <- new_blk_lst;
      Domain.DLS.set add_dls (Some (1, new_blk_lst))

(* let steal { steal_dls; blk_sz; blk_lst_arr; num_domains; _ } =
   let rec loop id head blk_lst =
     match blk_lst with
     | Null (* Next linked list *) ->
         let next_id = (id + 1) mod num_domains in
         loop next_id 0 blk_lst_arr.(next_id)
     | Ptr blk ->
         let { elems; a_fields } = !blk in
         let { next; _ } = Atomic.get a_fields in
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
   | Some (head, _prev_blk_lst, blk_lst) -> loop id head blk_lst *)

let steal_ext
    ({ steal_dls; blk_sz; blk_lst_arr; num_domains; notif_arr; _ } as t) =
  let rec loop (i, n, id) head blk_lst =
    Printf.printf "loop (%d, %d, %d) %d\n%!" i n id head;
    (* Finished scanning i iterations over bag *)
    if i > num_domains then (
      Printf.printf "Finish, got None\n";
      None)
    else if (* Finished checking one bag cycle *)
            n > num_domains then (
      Printf.printf "Done 1 bag cycle\n";
      loop (i + 1, 0, id) head blk_lst)
    else
      match blk_lst with
      | Null (* Next linked list *) ->
          let bit = Atomic.get notif_arr.(id) in
          (* Add was performed somewhere in this blk_lst *)
          if i > 1 && not bit then (
            (* Reset notif arr *)
            Array.iter (fun b -> Atomic.set b false) notif_arr;
            Printf.printf "Add happened, resetting\n";
            loop (0, 0, id) 0 blk_lst_arr.(id))
          else (
            if i = 1 then Atomic.set notif_arr.(id) true;
            let next_id = (id + 1) mod num_domains in
            Printf.printf "Next list id:%d\n" next_id;
            Format.printf "%a\n%a\n%!"
              (pp_print_t Format.pp_print_int)
              t
              (pp_print_blk_list Format.pp_print_int)
              blk_lst_arr.(next_id);
            loop (i, n + 1, next_id) 0 blk_lst_arr.(next_id))
      | Ptr blk ->
          let { elems; a_fields } = !blk in
          let { next; _ } = Atomic.get a_fields in
          if head >= blk_sz then (
            (* Set new steal head and advance next blk *)
            Domain.DLS.set steal_dls (Some (0, blk_lst, next));
            Printf.printf "Next blk\n";
            loop (i, n, id) 0 next)
          else
            (* Try to get item *)
            let item = Atomic.get elems.(head) in
            if
              Option.is_some item
              && Atomic.compare_and_set elems.(head) item None
            then (
              Printf.printf "FOUND\n";

              item)
            else (
              (* Need to update head only in DLS *)
              Printf.printf "Failed to get, continuing.\n";
              loop (i, n, id) (head + 1) blk_lst)
  in
  let id = (Domain.self () :> int) in
  match Domain.DLS.get steal_dls with
  | Some (_head, _prev_blk_lst, _blk_lst) ->
      Domain.DLS.set steal_dls (Some (0, Null, blk_lst_arr.(id)));
      loop (0, 0, id) 0 blk_lst_arr.(id) (* loop (0, 0, id) head blk_lst *)
  | None (* Initialize *) ->
      Domain.DLS.set steal_dls (Some (0, Null, blk_lst_arr.(id)));
      loop (0, 0, id) 0 blk_lst_arr.(id)

let try_remove_any ({ add_dls; blk_sz; _ } as t) =
  let rec loop head blk_lst =
    if head < 0 then
      match blk_lst with
      | Null (* Thread block list uninitialized *) -> steal_ext t
      | Ptr blk ->
          let { a_fields; _ } = !blk in
          let { next; _ } = Atomic.get a_fields in
          if next = Null (* Last block *) then steal_ext t
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
  | None (* Empty block list *) -> steal_ext t
  | Some (head, blk_lst) -> loop (head - 1) blk_lst

(* let delete_blk ({ steal_dls; _ } : 'a t) =
   match Domain.DLS.get steal_dls with
   | None -> assert false
   | Some (_, steal_prev_ptr, steal_blk_ptr) ->
       let steal_prev_ptr_ref = ref steal_prev_ptr in
       let steal_blk_ptr_ref = ref steal_blk_ptr in
       if steal_prev_ptr <> Null then (
         let steal_prev = !^steal_prev_ptr in
         let steal_prev_fields = Atomic.get steal_prev.a_fields in
         if
           (* Try mark steal_prev.next for steal_blk logical deletion *)
           Atomic.compare_and_set steal_prev.a_fields
             { steal_prev_fields with next = steal_blk_ptr }
             { steal_prev_fields with next = steal_blk_ptr; mark2 = true }
         then (
           (* Set mark1 on this steal_blk *)
           let steal_blk = !^steal_blk_ptr in
           let steal_blk_fields = Atomic.get steal_blk.a_fields in
           Atomic.set steal_blk.a_fields { steal_blk_fields with mark1 = true };
           if
             (* A concurrent delete_blk has marked the next blk for deletion *)
             let steal_blk_fields = Atomic.get steal_blk.a_fields in
             steal_blk_fields.mark2
           then
             (* Propogate mark1 *)
             match (Atomic.get steal_blk.a_fields).next with
             | Null -> assert false
             | steal_blk_next_ptr ->
                 let steal_blk_next = !^steal_blk_next_ptr in
                 let steal_blk_next_fields =
                   Atomic.get steal_blk_next.a_fields
                 in
                 Atomic.set steal_blk_next.a_fields
                   { steal_blk_next_fields with mark1 = true };
                 while
                   let steal_blk_fields = Atomic.get steal_blk.a_fields in
                   let steal_prev_fields = Atomic.get steal_prev.a_fields in
                   if steal_prev_fields.next <> steal_blk_ptr then ();
                   (* UpdateStealPrev *)
                   steal_prev_ptr = Null
                   || Atomic.compare_and_set steal_prev.a_fields
                        {
                          steal_prev_fields with
                          next = steal_blk_ptr;
                          mark2 = true;
                        }
                        { steal_blk_fields with mark1 = false }
                 do
                   ()
                 done;
                 steal_blk_ptr_ref := (Atomic.get steal_blk.a_fields).next);
         (* UpdateStealPrev *)
         ())
       else (
         steal_prev_ptr_ref := !steal_blk_ptr_ref;
         steal_blk_ptr_ref := (Atomic.get !^steal_blk_ptr.a_fields).next)
*)
