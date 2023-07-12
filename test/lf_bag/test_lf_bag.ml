open Lockfree

let pp_print_option pp_v ppf v =
  let pp_none ppf () = Format.pp_print_string ppf "None" in
  Format.pp_print_option ~none:pp_none pp_v ppf v

let _test () =
  let t = Lf_bag.create ~blk_sz:5 () in
  let add10 () =
    for i = 1 to 10 do
      Lf_bag.add t i
    done
  in
  let remove30 () =
    let lst = ref [] in
    for _ = 1 to 30 do
      lst := Lf_bag.try_remove_any t :: !lst
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
  Lf_bag.print_int_bag t;
  remove30 ();
  Format.printf "Removed %a\n"
    (pp_print_option Format.pp_print_int)
    (Lf_bag.try_remove_any t);
  Lf_bag.print_int_bag t

let test () = Alcotest.(check unit) "no errors" () (_test ())

let () =
  Alcotest.(run "LfBag" [ ("quicktest", [ test_case "quick" `Quick test ]) ])
