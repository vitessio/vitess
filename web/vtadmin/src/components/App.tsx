import React from 'react';

import style from './App.module.scss';
import logo from '../img/vitess-icon-color.svg';

export const App = () => {
  return (
    <div className={style.container}>
      <img src={logo} alt="logo" height={40} />
      <h1>VTAdmin</h1>
    </div>
  );
}
