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
  ignore (Lf_bag.try_remove_any t);
  add10 ();
  Lf_bag.print_int_bag t

let test () = Alcotest.(check unit) "no errors" () (_test ())

let test2 () =
  Alcotest.(check (option int))
    "no errors" (Some 1)
    (let b1 = Lf_bag.create () in
     let b2 = Lf_bag.create () in
     Lf_bag.add b1 1;
     let d =
       Domain.spawn (fun () ->
           Lf_bag.add b2 1;
           Lf_bag.try_remove_any b2)
     in
     Domain.join d)

let () =
  Alcotest.(
    run "LfBag"
      [
        ( "quicktest",
          [
            test_case "quick" `Quick test;
            test_case "thread_local storage" `Quick test2;
          ] );
      ])
