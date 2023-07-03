open Lockfree__.Lf_bag

let () =
  let t = S.create ~blk_sz:5 () in
  let add10 () =
    for i = 1 to 10 do
      S.add t i
    done
  in
  let remove30 () =
    let lst = ref [] in
    for _ = 1 to 30 do
      lst := S.try_remove_any t :: !lst
    done;
    let pp_sep ppf () = Format.pp_print_string ppf "; " in
    let pp_v =
      Format.pp_print_option
        ~none:(fun ppf _ -> Format.fprintf ppf "None")
        Format.pp_print_int
    in
    let pp_print_int_list = Format.(pp_print_list ~pp_sep pp_v) in
    Format.fprintf Format.std_formatter "@[[ %a ]@]@." pp_print_int_list !lst
  in
  let d1 = Domain.spawn add10 in
  let d2 = Domain.spawn add10 in
  add10 ();
  Domain.join d1;
  Domain.join d2;
  S.print_int_bag t;
  remove30 ();
  S.print_int_bag t
